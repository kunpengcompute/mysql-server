/* Copyright (c) 2000, 2019, Oracle and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/**
  @file
  Implementations of basic iterators, ie. those that have no children
  and don't take any refs (they typically read directly from a table
  in some way). See row_iterator.h.
*/

#include "sql/records.h"

#include <string.h>
#include <algorithm>
#include <atomic>
#include <new>

#include "my_base.h"
#include "my_dbug.h"
#include "my_inttypes.h"
#include "my_sys.h"
#include "sql/debug_sync.h"
#include "sql/handler.h"
#include "sql/item.h"
#include "sql/key.h"
#include "sql/opt_range.h"  // QUICK_SELECT_I
#include "sql/sql_class.h"  // THD
#include "sql/sql_const.h"
#include "sql/sql_executor.h"
#include "sql/sql_optimizer.h"
#include "sql/sql_sort.h"
#include "sql/sql_tmp_table.h"
#include "sql/table.h"
#include "sql/timing_iterator.h"
#include "sql/sql_parallel.h"
#include "sql/exchange_sort.h"
#include "sql/sql_parse.h"
#include "sql/mysqld.h"
#include "sql/exchange.h"

using std::string;
using std::vector;

/**
  Initialize a row iterator to perform full index scan in desired
  direction using the RowIterator interface

  This function has been added at late stage and is used only by
  UPDATE/DELETE. Other statements perform index scans using IndexScanIterator.

  @param thd          Thread handle
  @param table        Table to be accessed
  @param idx          index to scan
  @param reverse      Scan in the reverse direction
  @param qep_tab      If not NULL, used for record buffer and pushed condition

  @retval true   error
  @retval false  success
*/

unique_ptr_destroy_only<RowIterator> create_table_iterator_idx(
    THD *thd, TABLE *table, uint idx, bool reverse, QEP_TAB *qep_tab) {
  empty_record(table);

  ha_rows *examined_rows = nullptr;
  if (qep_tab != nullptr && qep_tab->join() != nullptr) {
    examined_rows = &qep_tab->join()->examined_rows;
  }

  if (reverse) {
    return NewIterator<IndexScanIterator<true>>(thd, table, idx,
                                                /*use_order=*/true, qep_tab,
                                                examined_rows);
  } else {
    return NewIterator<IndexScanIterator<false>>(thd, table, idx,
                                                 /*use_order=*/true, qep_tab,
                                                 examined_rows);
  }
}

template <bool Reverse>
IndexScanIterator<Reverse>::IndexScanIterator(THD *thd, TABLE *table, int idx,
                                              bool use_order, QEP_TAB *qep_tab,
                                              ha_rows *examined_rows)
    : TableRowIterator(thd, table),
      m_record(table->record[0]),
      m_idx(idx),
      m_use_order(use_order),
      m_qep_tab(qep_tab),
      m_examined_rows(examined_rows) {}

template <bool Reverse>
IndexScanIterator<Reverse>::~IndexScanIterator() {
  if (table() && table()->key_read) {
    table()->set_keyread(false);
  }
}

template <bool Reverse>
bool IndexScanIterator<Reverse>::Init() {
  if (!table()->file->inited) {
    if (table()->covering_keys.is_set(m_idx) && !table()->no_keyread) {
      table()->set_keyread(true);
    }

    int error = table()->file->ha_index_init(m_idx, m_use_order);
    if (error) {
      PrintError(error);
      return true;
    }

    if (set_record_buffer(m_qep_tab)) {
      return true;
    }
  }
  m_first = true;
  return false;
}

// Doxygen gets confused by the explicit specializations.

//! @cond
template <>
int IndexScanIterator<false>::Read() {  // Forward read.
  int error;
  if (m_first) {
    error = table()->file->ha_index_first(m_record);
    m_first = false;
  } else {
    error = table()->file->ha_index_next(m_record);
  }
  if (error) return HandleError(error);
  if (m_examined_rows != nullptr) {
    ++*m_examined_rows;
  }
  return 0;
}

template <>
int IndexScanIterator<true>::Read() {  // Backward read.
  int error;
  if (m_first) {
    error = table()->file->ha_index_last(m_record);
    m_first = false;
  } else {
    error = table()->file->ha_index_prev(m_record);
  }
  if (error) return HandleError(error);
  if (m_examined_rows != nullptr) {
    ++*m_examined_rows;
  }
  return 0;
}
//! @endcond

template <bool Reverse>
vector<string> IndexScanIterator<Reverse>::DebugString() const {
  DBUG_ASSERT(table()->file->pushed_idx_cond == nullptr);

  const KEY *key = &table()->key_info[m_idx];
  string str =
      string("Index scan on ") + table()->alias + " using " + key->name;
  if (Reverse) {
    str += " (reverse)";
  }
  str += table()->file->explain_extra();
  return {str};
}

template class IndexScanIterator<true>;
template class IndexScanIterator<false>;

/**
  setup_read_record is used to scan by using a number of different methods.
  Which method to use is set-up in this call so that you can fetch rows
  through the resulting row iterator afterwards.

  @param thd      Thread handle
  @param table    Table the data [originally] comes from; if NULL,
    'table' is inferred from 'qep_tab'; if non-NULL, 'qep_tab' must be NULL.
  @param qep_tab  QEP_TAB for 'table', if there is one; we may use
    qep_tab->quick() as data source
  @param disable_rr_cache
    Don't use caching in SortBufferIndirectIterator (used by sort-union
    index-merge which produces rowid sequences that are already ordered)
  @param ignore_not_found_rows
    Ignore any rows not found in reference tables, as they may already have
    been deleted by foreign key handling. Only relevant for methods that need
    to look up rows in tables (those marked “Indirect”).
  @param examined_rows
    If non-nullptr, the iterator will increase this variable by the number of
    examined rows. If nullptr, will use qep_tab->join()->examined_rows
    if possible.
  @param using_table_scan
    If non-nullptr, will be whether a TableScanIterator was chosen.
 */
unique_ptr_destroy_only<RowIterator> create_table_iterator(
    THD *thd, TABLE *table, QEP_TAB *qep_tab, bool disable_rr_cache,
    bool ignore_not_found_rows, ha_rows *examined_rows,
    bool *using_table_scan, bool *pq_replace_iterator) {
  // If only 'table' is given, assume no quick, no condition.
  DBUG_ASSERT(!(table && qep_tab));
  if (!table) table = qep_tab->table();
  empty_record(table);
  if (using_table_scan != nullptr) {
    *using_table_scan = false;
  }

  bool could_replace_iter = false;
  if (examined_rows == nullptr && qep_tab != nullptr &&
      qep_tab->join() != nullptr) {
    examined_rows = &qep_tab->join()->examined_rows;
  }

  QUICK_SELECT_I *quick = qep_tab ? qep_tab->quick() : nullptr;
  unique_ptr_destroy_only<RowIterator> iterator;

  if (table->unique_result.io_cache &&
      my_b_inited(table->unique_result.io_cache)) {
    DBUG_PRINT("info", ("using SortFileIndirectIterator"));
    iterator =
        NewIterator<SortFileIndirectIterator>(
            thd, table, table->unique_result.io_cache, !disable_rr_cache,
            ignore_not_found_rows, examined_rows);
    table->unique_result.io_cache =
        nullptr;  // Now owned by SortFileIndirectIterator.
  } else if (quick) {
    DBUG_PRINT("info", ("using IndexRangeScanIterator"));
    iterator = NewIterator<IndexRangeScanIterator>(thd, table, quick, qep_tab,
                                               examined_rows);
    could_replace_iter = true;
  } else if (table->unique_result.has_result_in_memory()) {
    /*
      The Unique class never puts its results into table->sort's
      Filesort_buffer.
    */
    DBUG_ASSERT(!table->unique_result.sorted_result_in_fsbuf);
    DBUG_PRINT("info", ("using SortBufferIndirectIterator (unique)"));
    iterator = NewIterator<SortBufferIndirectIterator>(
        thd, table, &table->unique_result, ignore_not_found_rows,
        examined_rows);
  } else if (qep_tab != nullptr && qep_tab->table_ref != nullptr &&
             qep_tab->table_ref->is_recursive_reference()) {
    iterator =
        NewIterator<FollowTailIterator>(thd, table, qep_tab, examined_rows);
    qep_tab->recursive_iterator =
        down_cast<FollowTailIterator *>(iterator->real_iterator());
  } else {
    DBUG_PRINT("info", ("using TableScanIterator"));
    if (using_table_scan != nullptr) {
      *using_table_scan = true;
    }
    could_replace_iter = true;
    iterator = NewIterator<TableScanIterator>(thd, table, qep_tab, examined_rows);
  }

  if (pq_replace_iterator) *pq_replace_iterator = could_replace_iter;
  return iterator;
}

unique_ptr_destroy_only<RowIterator> init_table_iterator(
    THD *thd, TABLE *table, QEP_TAB *qep_tab, bool disable_rr_cache,
    bool ignore_not_found_rows) {
  unique_ptr_destroy_only<RowIterator> iterator = create_table_iterator(
      thd, table, qep_tab, disable_rr_cache, ignore_not_found_rows,
      /*examined_rows=*/nullptr, /*using_table_scan=*/nullptr);
  if (iterator->Init()) {
    return nullptr;
  }
  return iterator;
}

/**
  The default implementation of unlock-row method of RowIterator,
  used in all access methods except EQRefIterator.
*/
void TableRowIterator::UnlockRow() { m_table->file->unlock_row(); }

void TableRowIterator::SetNullRowFlag(bool is_null_row) {
  if (is_null_row) {
    m_table->set_null_row();
  } else {
    m_table->reset_null_row();
  }
}

int TableRowIterator::HandleError(int error) {
  if (thd()->killed) {
    thd()->send_kill_message();
    return 1;
  }

  if (error == HA_ERR_END_OF_FILE || error == HA_ERR_KEY_NOT_FOUND) {
    m_table->set_no_row();
    return -1;
  } else {
    PrintError(error);
    return 1;
  }
}

void TableRowIterator::PrintError(int error) {
  m_table->file->print_error(error, MYF(0));
}

void TableRowIterator::StartPSIBatchMode() {
  m_table->file->start_psi_batch_mode();
}

void TableRowIterator::EndPSIBatchModeIfStarted() {
  m_table->file->end_psi_batch_mode_if_started();
}

IndexRangeScanIterator::IndexRangeScanIterator(THD *thd, TABLE *table,
                                               QUICK_SELECT_I *quick,
                                               QEP_TAB *qep_tab,
                                               ha_rows *examined_rows)
    : TableRowIterator(thd, table),
      m_quick(quick),
      m_qep_tab(qep_tab),
      m_examined_rows(examined_rows) {}

bool IndexRangeScanIterator::Init() {
  /*
    Only attempt to allocate a record buffer the first time the handler is
    initialized.
  */
  const bool first_init = !table()->file->inited;

  int error = m_quick->reset();
  if (error) {
    // Ensures error status is propagated back to client.
    (void)report_handler_error(table(), error);
    return true;
  }

  if (first_init && table()->file->inited && set_record_buffer(m_qep_tab))
    return true; /* purecov: inspected */

  m_seen_eof = false;
  return false;
}

int IndexRangeScanIterator::Read() {
  if (m_seen_eof) {
    return -1;
  }

  int tmp;
  while ((tmp = m_quick->get_next())) {
    if (thd()->killed || (tmp != HA_ERR_RECORD_DELETED)) {
      int error_code = HandleError(tmp);
      if (error_code == -1) {
        m_seen_eof = true;
      }
      return error_code;
    }
  }

  if (m_examined_rows != nullptr) {
    ++*m_examined_rows;
  }
  return 0;
}

vector<string> IndexRangeScanIterator::DebugString() const {
  // TODO: Convert QUICK_SELECT_I to RowIterator so that we can get
  // better outputs here (similar to dbug_dump()).
  String str;
  m_quick->add_info_string(&str);
  string ret = string("Index range scan on ") + table()->alias + " using " +
               to_string(str);
  if (table()->file->pushed_idx_cond != nullptr) {
    ret += ", with index condition: " +
           ItemToString(table()->file->pushed_idx_cond);
  }
  ret += table()->file->explain_extra();
  return {ret};
}
 
ParallelScanIterator::ParallelScanIterator(THD *thd,
        QEP_TAB *tab, TABLE *table,
        ha_rows *examined_rows, 
        JOIN *join, Gather_operator *gather,
        bool stab_output, uint ref_length)
  : TableRowIterator(thd, table),
    m_record(table->record[0]),
    m_examined_rows(examined_rows),
    m_dop(gather->m_dop),
    m_join(join),
    m_gather(gather),
    m_record_gather(nullptr),
    m_order(nullptr),
    m_tab(tab),
    m_stable_sort(stab_output),
    m_ref_length(ref_length)
   {thd->pq_iterator = this;}

/**
 * construct filesort on leader when needing stab_output or merge_sort
 *
 * @retavl: false if success, and otherwise true
 */
bool ParallelScanIterator::pq_make_filesort(Filesort **sort)
{
  *sort= NULL;

  /** construct sort order based on group */
  if (m_join->pq_rebuilt_group)
  {
    DBUG_ASSERT( m_join->select_lex->saved_group_list_ptrs);
    restore_list(m_join->select_lex->saved_group_list_ptrs, m_join->select_lex->group_list);
    m_order = restore_optimized_group_order(m_join->select_lex->group_list,
                                            m_join->saved_optimized_vars.optimized_group_flags);
  } else {
    /**
     * if sorting is built after the first rewritten table, then
     * we have no need to rebuilt the sort order on leader, because
     * leader will do SortingIterator.
     */
    if (m_join->pq_last_sort_idx >= (int)m_join->tables &&
        m_join->qep_tab[m_join->pq_last_sort_idx].filesort != nullptr)
    {
      return false;
    } else {
      if ((m_order= m_join->order) == nullptr) {
        if (m_join->m_ordered_index_usage == JOIN::ORDERED_INDEX_ORDER_BY
            && m_join->select_lex->saved_order_list_ptrs)
        {
          restore_list(m_join->select_lex->saved_order_list_ptrs, m_join->select_lex->order_list);
          m_order= restore_optimized_group_order(m_join->select_lex->order_list,
                                                 m_join->saved_optimized_vars.optimized_order_flags);
        } else {
          std::vector<std::string> used_key_fields;
          if (get_table_key_fields(&m_join->qep_tab0[m_tab->pos], used_key_fields) ||
              DBUG_EVALUATE_IF("pq_msort_error1", true, false))
            return true;

          if (set_key_order(m_tab, used_key_fields, &m_order,
                            &m_join->ref_items[REF_SLICE_PQ_TMP]) ||
              DBUG_EVALUATE_IF("pq_msort_error2", true, false))
            return true;
        }
      }
    }
  }

  /** support stable sort on TABLE/INDEX SCAN */
  if (m_order || m_stable_sort)
  {
    *sort= m_tab->filesort;
    if(!(*sort)) {
      (*sort)= new (m_join->thd->pq_mem_root) Filesort(m_join->thd, m_tab->table(), false,
          m_order, HA_POS_ERROR, false, false, false);
      if (!(*sort) || DBUG_EVALUATE_IF("pq_msort_error3", true, false))
        return true;
    }
  }
  return false;
}

/**
 * init the mq_record_gather
 */
bool ParallelScanIterator::pq_init_record_gather()
{
  THD *thd= m_join->thd;
  Filesort *sort= NULL;
  if (pq_make_filesort(&sort))
    return true;
  m_record_gather= new (thd->pq_mem_root) MQ_record_gather(thd, m_tab);
  if (!m_record_gather ||
      m_record_gather->mq_scan_init(sort, m_gather->m_dop, m_ref_length, m_stable_sort) ||
      DBUG_EVALUATE_IF("pq_msort_error4", true, false))
    return true;

  /** set each worker's MQ_handle */
  for (uint i= 0; i < m_gather->m_dop; i++)
  {
    m_gather->m_workers[i]->m_handle=
        m_record_gather->m_exchange->get_mq_handle(i);
  }
  return false;
}

/**
 * launch worker threads
 *
 * @retval: false if success, and otherwise true
 */
bool ParallelScanIterator::pq_launch_worker()
{
  THD *thd= m_join->thd;
  DBUG_ASSERT(thd == current_thd);

  Gather_operator *gather= m_tab->gather;
  PQ_worker_manager **workers= gather->m_workers;
  int launch_workers = 0;

  /** when workers encounter error during execution, directly abort the parallel execution */
  for (uint i= 0; i < m_gather->m_dop; i++)
  {
    DBUG_ASSERT(!workers[i]->thd_worker &&
                (workers[i]->m_status == PQ_worker_state::INIT));
    if (thd->is_error() || thd->pq_error) goto err;
    my_thread_handle id;
    id.thread= 0;
    /**
     * pq_worker_error8: all workers are fialed to landuch
     * pq_worker_error9: worker's id in [0, 2, 4, ..] are failed to lanuch
     */
    if (DBUG_EVALUATE_IF("pq_worker_error8", false, true) &&
        DBUG_EVALUATE_IF("pq_worker_error9", (i % 2), true)) {
      mysql_thread_create(key_thread_parallel_query, &id, NULL, pq_worker_exec, (void *) workers[i]);
    }
    workers[i]->thread_id= id;
    int expected_status = PQ_worker_state::READY |
                          PQ_worker_state::COMPELET |
                          PQ_worker_state::ERROR;
    if (id.thread != 0)
    {
      /** Record the thread id so that we can later determine whether the thread started */
      workers[i]->m_active= workers[i]->wait_for_status(thd, expected_status);
      /** partial workers may fail before execution */
      if (!workers[i]->m_active ||
          DBUG_EVALUATE_IF("pq_worker_error7", (i >= m_gather->m_dop / 2), false)) {
        goto err;
      }
      launch_workers++;
    } else {
      sql_print_warning("worker %d has failed to start up\n", i);
      MQueue_handle *mq_handler= m_record_gather->m_exchange->get_mq_handle(i);
      if (mq_handler)
        mq_handler->set_datched_status(MQ_HAVE_DETACHED);
    }
  }
  /** if all workers are not launched, then directly return false */
  if (!launch_workers) goto err;
  return false;

  err:
  for (uint i= 0; i < m_gather->m_dop; i++) {
    if (workers[i]->thread_id.thread &&
        workers[i]->thd_worker) {
      workers[i]->thd_worker->pq_error= true;
    }
  }
  return true;
}

/**
 * wait all workers finish their execution
 */
void ParallelScanIterator::pq_wait_workers_finished()
{
  THD *leader_thd= m_join->thd;
  DBUG_ASSERT(leader_thd == current_thd);

  /**
   * leader first detached the message queue, and then wait workers finish
   * the execution. The reason for detach MQ is that leader has fetched the
   * satisfied #records (e.g., limit operation).
   */
  if (m_record_gather) {
    Exchange *exchange = m_record_gather->m_exchange;
    MQueue_handle *m_handle = nullptr;
    for (uint i = 0; i < m_gather->m_dop; i++) {
      if ((m_handle = exchange->get_mq_handle(i))) {
        m_handle->set_datched_status(MQ_HAVE_DETACHED);
      }
    }
  }

  /**
   * wait all such workers to finish execution, two conditions must meet:
   *  c1: the worker thread has been created
   *  c2: the worker has not yet finished
   */
  int expected_status = PQ_worker_state::COMPELET | PQ_worker_state::ERROR;
  for (uint i= 0; i < m_gather->m_dop; i++)
  {
    if (m_gather->m_workers[i]->thread_id.thread != 0)               //c1
    {
      if (m_gather->m_workers[i]->m_active &&
          !( ((unsigned int)m_gather->m_workers[i]->m_status) & PQ_worker_state::COMPELET)) {
        m_gather->m_workers[i]->wait_for_status(leader_thd, expected_status);
      }
      my_thread_join(&m_gather->m_workers[i]->thread_id, NULL);
    }
  }
}

int ParallelScanIterator::pq_error_code()
{
  THD *thd = m_join->thd;

  if (m_gather->m_ha_err == HA_ERR_TABLE_DEF_CHANGED) {
    m_gather->m_ha_err = 0;
    return HA_ERR_TABLE_DEF_CHANGED;
  }

  if (thd->is_killed()) {
    thd->send_kill_message();
  }

  /** collect worker threads status from DA info */
  JOIN *tmplate_join= m_gather->m_template_join;
  THD *temp_thd= tmplate_join->thd;
  thd->pq_status_reset();
  thd->pq_merge_status(temp_thd);
  Diagnostics_area *da= temp_thd->get_stmt_da();
  if (temp_thd->is_error())
  {
    temp_thd->raise_condition(da->mysql_errno(), da->returned_sqlstate(),
                              Sql_condition::SL_ERROR ,da->message_text());
  }

  if (da->cond_count() > 0)
  {
    Diagnostics_area::Sql_condition_iterator it= da->sql_conditions();
    const Sql_condition *cond;
    while ((cond= it++))
    {
      thd->raise_condition(cond->mysql_errno(), NULL,
                           cond->severity(), cond->message_text());
    }
  }
  /**  output parallel error code */
  if (!temp_thd->is_error() && !thd->is_error()
      && thd->pq_error  && !thd->running_explain_analyze)
  {
    my_error(ER_PARALLEL_EXEC_ERROR, MYF(0));
  }
  return 1;
}

bool ParallelScanIterator::Init() {
  DBUG_ASSERT(current_thd == m_join->thd);
  if (m_gather->init() ||                /** cur innodb data,
                                             should be called first(will change dop based on split count) */
      pq_init_record_gather() ||         /** init mq_record_gather */
      pq_launch_worker() ||              /** launch worker threads */
      DBUG_EVALUATE_IF("pq_worker_error6", true, false))
  {
    m_join->thd->pq_error= true;
    return true;
  }
  return false;
}

int ParallelScanIterator::Read() {
  /** kill query */
  if (m_join->thd->is_killed())
  {
    m_join->thd->send_kill_message();
    return 1;
  }
  /** fetch message from MQ to table->record[0] */
  if (m_record_gather->mq_scan_next())
    return 0;
  return -1;

}

int ParallelScanIterator::End() {
  /** wait all workers to finish their execution */
  pq_wait_workers_finished();
  /** output error code */
  return pq_error_code();
}

ParallelScanIterator::~ParallelScanIterator() {
  table()->file->ha_index_or_rnd_end();
  /** cleanup m_record_gather */
  if (m_record_gather) {
    m_record_gather->mq_scan_end();
  }
}

/** Currently, parallel query supports simple "explain format=tree",
 *  and shows the parallel query cost in the future. */
vector<string> ParallelScanIterator::DebugString() const {
  DBUG_ASSERT(table()->file->pushed_idx_cond == nullptr);
  DBUG_ASSERT(m_tab->old_table());

  return {string("Parallel scan on <temporary>") + table()->file->explain_extra()};
}

vector<RowIterator::Child> ParallelScanIterator::children() const {
  if (m_gather->iterator.get() == NULL) {
    return std::vector<Child>{{m_gather->m_workers[0]->thd_worker->lex->unit->m_root_iterator.get(), ""}};
  }
  
  return std::vector<Child>{{m_gather->iterator.get(), ""}};
}

PQblockScanIterator::PQblockScanIterator(THD *thd, TABLE *table,
                                           uchar* record,
                                           ha_rows *examined_rows,
                                           Gather_operator *gather,
                                           bool need_rowid)
  : TableRowIterator(thd, table),
    m_record(record),
    m_examined_rows(examined_rows),
    m_pq_ctx(gather->m_pq_ctx),
    keyno(gather->keyno),
    m_gather(gather),
    m_need_rowid(need_rowid)
{thd->pq_iterator = this;}

bool PQblockScanIterator::Init() {
  table()->file->pq_worker_scan_init(keyno, m_pq_ctx);
  return false;
}

int PQblockScanIterator::End() {
  DBUG_ASSERT(thd() && thd()->pq_leader);
  if (m_gather) m_gather->signalAll();
  return -1;
}

PQblockScanIterator::~PQblockScanIterator() {}

int PQblockScanIterator::Read() {
  int tmp;
  while((tmp = table()->file->ha_pq_next(m_record, m_pq_ctx))) {
  /*
    ha_rnd_next can return RECORD_DELETED for MyISAM when one thread is
    reading and another deleting without locks.
  */
  if (tmp == HA_ERR_RECORD_DELETED && !thd()->killed) continue;
    return HandleError(tmp);
  }

  if (m_examined_rows != nullptr) {
    ++*m_examined_rows;
  }
  // write row_id into file
  if (m_need_rowid) {
    DBUG_ASSERT(table()->file->ht->db_type == DB_TYPE_INNODB);
    DBUG_ASSERT(table()->record[0] == m_record);
    table()->file->position(m_record);
  }

  return 0;
}

/** explain format=tree */
vector<string> PQblockScanIterator::DebugString() const {
  const KEY *key = &table()->key_info[keyno];
  int tab_idx = m_gather->m_template_join->pq_tab_idx;
  DBUG_ASSERT(tab_idx >= (int)m_gather->m_template_join->const_tables
              && m_gather->m_template_join->qep_tab[tab_idx].do_parallel_scan);
  QEP_TAB *tab = &m_gather->m_template_join->qep_tab[tab_idx];

  string str;
  switch (tab->type()) {
    case JT_ALL:
      str = string("PQblock scan on ") + table()->alias;
      break;
    case JT_RANGE:
      str = string("PQblock range scan on ") + table()->alias + " using " + key->name;
      if (table()->file->pushed_idx_cond != nullptr) {
        str += string(", with index condition: ") +
              ItemToString(table()->file->pushed_idx_cond);
      }
      break;
    case JT_REF:
      str = string("PQblock lookup on ") + table()->alias + string(" using ") +
               key->name + " (" +
               RefToString(tab->ref(), key, /*include_nulls=*/false);
      if (tab->m_reversed_access) {
        str += string("; iterate backwards");
      }
      str += string(")");
      if (table()->file->pushed_idx_cond != nullptr) {
        str += string(", with index condition: ") +
              ItemToString(table()->file->pushed_idx_cond);
      }
      break;
    case JT_INDEX_SCAN:
      str = string("PQblock scan on ") + table()->alias + " using " + key->name;
      if (tab->m_reversed_access) {
        str += string(" (reverse)");
      }
      break;
    default:
      DBUG_ASSERT(0);
      break;
  }
  str += table()->file->explain_extra();
  return {str};
 }  

void PQExplainIterator::copy(RowIterator* src_iterator) {
    set_expected_rows(src_iterator->expected_rows());
    set_estimated_cost(src_iterator->estimated_cost());
    str = src_iterator->DebugString();
    time_string = src_iterator->TimingString();

    for (const RowIterator::Child &child : src_iterator->children()) {
        unique_ptr_destroy_only<PQExplainIterator> it(new PQExplainIterator()); 
        it->copy(child.iterator);
        ch.push_back({it.get(), child.description});
        iter.push_back(std::move(it));     
    }
}

TableScanIterator::TableScanIterator(THD *thd, TABLE *table, QEP_TAB *qep_tab,
                                     ha_rows *examined_rows)
    : TableRowIterator(thd, table),
      m_record(table->record[0]),
      m_qep_tab(qep_tab),
      m_examined_rows(examined_rows) {}

TableScanIterator::~TableScanIterator() {
  if (table()->file != nullptr) {
    table()->file->ha_index_or_rnd_end();
  }
}

bool TableScanIterator::Init() {
  /*
    Only attempt to allocate a record buffer the first time the handler is
    initialized.
  */
  const bool first_init = !table()->file->inited;

  int error=0;
  error = table()->file->ha_rnd_init(1);
  if (error) {
    PrintError(error);
    return true;
  }

  if (first_init && set_record_buffer(m_qep_tab))
    return true; /* purecov: inspected */

  return false;
}

int TableScanIterator::Read() {
  int tmp;
  while ((tmp = table()->file->ha_rnd_next(m_record))) {
    /*
      ha_rnd_next can return RECORD_DELETED for MyISAM when one thread is
      reading and another deleting without locks.
    */
      if (tmp == HA_ERR_RECORD_DELETED && !thd()->killed) continue;
      return HandleError(tmp);
  }

  if (m_examined_rows != nullptr) {
    ++*m_examined_rows;
  }
  return 0;
}

vector<string> TableScanIterator::DebugString() const {
  DBUG_ASSERT(table()->file->pushed_idx_cond == nullptr);
  return {string("Table scan on ") + table()->alias +
          table()->file->explain_extra()};
}

FollowTailIterator::FollowTailIterator(THD *thd, TABLE *table, QEP_TAB *qep_tab,
                                       ha_rows *examined_rows)
    : TableRowIterator(thd, table),
      m_record(table->record[0]),
      m_qep_tab(qep_tab),
      m_examined_rows(examined_rows) {}

FollowTailIterator::~FollowTailIterator() {
  if (table()->file != nullptr) {
    table()->file->ha_index_or_rnd_end();
  }
}

bool FollowTailIterator::Init() {
  // BeginMaterialization() must be called before this.
  DBUG_ASSERT(m_stored_rows != nullptr);

  /*
    Only attempt to allocate a record buffer the first time the handler is
    initialized.
  */
  const bool first_init = !table()->file->inited;

  if (first_init) {
    // The first Init() call at the start of a new WITH RECURSIVE
    // execution. MaterializeIterator calls ha_index_or_rnd_end()
    // before each iteration, which sets file->inited = false,
    // so we can use that as a signal.
    if (!table()->is_created()) {
      // Recursive references always refer to a temporary table,
      // which do not exist at resolution time; thus, we need to
      // connect to it on first run here.
      if (open_tmp_table(table())) {
        return true;
      }
    }

    int error = table()->file->ha_rnd_init(true);
    if (error) {
      PrintError(error);
      return true;
    }

    if (first_init && set_record_buffer(m_qep_tab))
      return true; /* purecov: inspected */

    // The first seen record will start a new iteration.
    m_read_rows = 0;
    m_recursive_iteration_count = 0;
    m_end_of_current_iteration = 0;
  } else {
    // Just continue where we left off last time.
  }

  return false;
}

int FollowTailIterator::Read() {
  if (m_read_rows == *m_stored_rows) {
    /*
      Return EOF without even checking if there are more rows
      (there isn't), so that we can continue reading when there are.
      There are two underlying reasons why we need to do this,
      depending on the storage engine in use:

      1. For both MEMORY and InnoDB, when they report EOF,
         the scan stays blocked at EOF forever even if new rows
         are inserted later. (InnoDB has a supremum record, and
         MEMORY increments info->current_record unconditionally.)

      2. Specific to MEMORY, inserting records that are deduplicated
         away can corrupt cursors that hit EOF. Consider the following
         scenario:

         - write 'A'
         - write 'A': allocates a record, hits a duplicate key error, leaves
           the allocated place as "deleted record".
         - init scan
         - read: finds 'A' at #0
         - read: finds deleted record at #1, properly skips over it, moves to
           EOF
         - even if we save the read position at this point, it's "after #1"
         - close scan
         - write 'B': takes the place of deleted record, i.e. writes at #1
         - write 'C': writes at #2
         - init scan, reposition at saved position
         - read: still after #1, so misses 'B'.

         In this scenario, the table is formed of real records followed by
         deleted records and then EOF.

       To avoid these problems, we keep track of the number of rows in the
       table by holding the m_stored_rows pointer into the MaterializeIterator,
       and simply avoid hitting EOF.
     */
    return -1;
  }

  if (m_read_rows == m_end_of_current_iteration) {
    // We have started a new iteration. Check to see if we have passed the
    // user-set limit.
    if (++m_recursive_iteration_count >
        thd()->variables.cte_max_recursion_depth) {
      my_error(ER_CTE_MAX_RECURSION_DEPTH, MYF(0), m_recursive_iteration_count);
      return 1;
    }
    m_end_of_current_iteration = *m_stored_rows;

#ifdef ENABLED_DEBUG_SYNC
    if (m_recursive_iteration_count == 4) {
      DEBUG_SYNC(thd(), "in_WITH_RECURSIVE");
    }
#endif
  }

  // Read the actual row.
  //
  // We can never have MyISAM here, so we don't need the checks
  // for HA_ERR_RECORD_DELETED that TableScanIterator has.
  int err = table()->file->ha_rnd_next(m_record);
  if (err) {
    return HandleError(err);
  }

  ++m_read_rows;

  if (m_examined_rows != nullptr) {
    ++*m_examined_rows;
  }
  return 0;
}

vector<string> FollowTailIterator::DebugString() const {
  DBUG_ASSERT(table()->file->pushed_idx_cond == nullptr);
  return {string("Scan new records on ") + table()->alias};
}

bool FollowTailIterator::RepositionCursorAfterSpillToDisk() {
  return reposition_innodb_cursor(table(), m_read_rows);
}
