/* Copyright (c) 2021, Huawei and/or its affiliates. All rights reserved.

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


#include "sql/pq_condition.h"
#include "sql/sql_parallel.h"
#include "sql/item_strfunc.h"
#include "sql/item_sum.h"
#include "sql/sql_optimizer.h"
#include "sql/mysqld.h"
#include "sql/sql_tmp_table.h"
#include "sql/opt_range.h"


static const enum_field_types NO_PQ_SUPPORTED_FIELD_TYPES [] = {
  MYSQL_TYPE_TINY_BLOB,
  MYSQL_TYPE_MEDIUM_BLOB,
  MYSQL_TYPE_BLOB,
  MYSQL_TYPE_LONG_BLOB,
  MYSQL_TYPE_JSON,
  MYSQL_TYPE_GEOMETRY
};

static const Item_sum::Sumfunctype NO_PQ_SUPPORTED_AGG_FUNC_TYPES [] = {
  Item_sum::COUNT_DISTINCT_FUNC,
  Item_sum::SUM_DISTINCT_FUNC,
  Item_sum::AVG_DISTINCT_FUNC,
  Item_sum::GROUP_CONCAT_FUNC,
  Item_sum::JSON_AGG_FUNC,
  Item_sum::UDF_SUM_FUNC ,
  Item_sum::STD_FUNC,
  Item_sum::VARIANCE_FUNC
};

static const Item_func::Functype NO_PQ_SUPPORTED_FUNC_TYPES [] = {
  Item_func::MATCH_FUNC,
  Item_func::SUSERVAR_FUNC,
  Item_func::FUNC_SP,
  Item_func::JSON_FUNC,
  Item_func::SUSERVAR_FUNC,
  Item_func::UDF_FUNC,
  Item_func::XML_FUNC
};

static const char* NO_PQ_SUPPORTED_FUNC_ARGS [] = {
  "rand",
  "json_valid",
  "json_length",
  "json_type",
  "json_contains_path",
  "json_unquote",
  "st_distance",
  "get_lock",
  "is_free_lock",
  "is_used_lock",
  "release_lock",
  "sleep",
  "xml_str",
  "json_func",
  "weight_string",  // Data truncation (MySQL BUG)
  "des_decrypt"     // Data truncation
};

static const char* NO_PQ_SUPPORTED_FUNC_NO_ARGS [] = {
  "release_all_locks"
};

static const Item_ref::Ref_Type NO_PQ_SUPPORTED_REF_TYPES[] = {
    Item_ref::OUTER_REF,
    Item_ref::AGGREGATE_REF
};

/**
 * return true when type is a not_supported_field; return false otherwise.
 */
static bool pq_not_support_datatype(enum_field_types type) {
  for (const enum_field_types &field_type : NO_PQ_SUPPORTED_FIELD_TYPES) {
    if (type == field_type) {
      return true;
    }
  }

  return false;
}

/**
 * check PQ supported function type
 */
static bool pq_not_support_functype(Item_func::Functype type) {
  for (const Item_func::Functype &func_type : NO_PQ_SUPPORTED_FUNC_TYPES) {
    if (type == func_type) {
      return true;
    }
  }

  return false;
}

/**
 * check PQ supported function 
 */
static bool pq_not_support_func(Item_func *func) {
  if (pq_not_support_functype(func->functype())) {
    return true;
  }

  for (const char* funcname : NO_PQ_SUPPORTED_FUNC_ARGS) {
    if (!strcmp(func->func_name(), funcname)) {
      return true;
    }
  }

  for (const char* funcname : NO_PQ_SUPPORTED_FUNC_NO_ARGS) {
    if (!strcmp(func->func_name(), funcname)) {
      return true;
    }
  }
  
  return false;
}

/**
 * check PQ support aggregation function
 */
static bool pq_not_support_aggr_functype(Item_sum::Sumfunctype type) {
  for (const Item_sum::Sumfunctype &sum_func_type : NO_PQ_SUPPORTED_AGG_FUNC_TYPES) {
    if (sum_func_type == type) {
      return true;
    }
  }

  return false;
}

/**
 * check PQ supported ref function
 */
static bool pq_not_support_ref(Item_ref *ref) {
  Item_ref::Ref_Type type = ref->ref_type();
  for (auto &ref_type : NO_PQ_SUPPORTED_REF_TYPES) {
    if (type == ref_type) {
      return true;
    }
  }

  return false;
}

typedef bool (*PQ_CHECK_ITEM_FUN)(Item *item);

struct PQ_CHECK_ITEM_TYPE {
  Item::Type item_type;
  PQ_CHECK_ITEM_FUN fun_ptr;
};

static bool check_pq_support_fieldtype(Item *item);

static bool check_pq_support_fieldtype_of_field_item(Item *item) {
  Field *field = static_cast<Item_field *>(item)->field;
  DBUG_ASSERT(field);
  // not supported for generated column
  if (field && (field->is_gcol() ||
                pq_not_support_datatype(field->type()))) {
    return false;
  }  
  
  return true;
}

static bool check_pq_support_fieldtype_of_func_item(Item *item) {
  Item_func *func = static_cast<Item_func *>(item);
  DBUG_ASSERT(func);

  // check func type
  if (pq_not_support_func(func)) {
    return false;
  }

  // the case of Item_func_make_set
  if (!strcmp(func->func_name(), "make_set")) {
    Item *arg_item = down_cast<Item_func_make_set *>(func)->item;
    if (arg_item && !check_pq_support_fieldtype(arg_item)) {
      return false;
    }
  }

  // check func args type
  for (uint i =0; i < func->arg_count; i++) {
    //c1: Item_func::args contain aggr. function, (i.e., Item_sum)
    //c2: args contain unsupported fields
    Item *arg_item = func->arguments()[i];
    if (!arg_item || arg_item->type() == Item::SUM_FUNC_ITEM ||         //c1
        !check_pq_support_fieldtype(arg_item)) {            //c2
      return false;
    }
  }

  //the case of Item_equal
  if (func->functype() == Item_func::MULT_EQUAL_FUNC) {
    Item_equal *item_equal = down_cast<Item_equal *>(item);
    DBUG_ASSERT(item_equal);

    // check const_item
    Item *const_item = item_equal->get_const();
    if (const_item &&
        (const_item->type() == Item::SUM_FUNC_ITEM ||         //c1
          !check_pq_support_fieldtype(const_item))) {          //c2
      return false;
    }

    // check fields
    Item *field_item = nullptr;
    List<Item_field> fields = item_equal->get_fields();
    List_iterator_fast<Item_field> it(fields);
    for (size_t i = 0; (field_item = it++); i++) {
      if (!check_pq_support_fieldtype(field_item)) {
        return false;
      }
    }
  }

  return true;
}

static bool check_pq_support_fieldtype_of_cond_item(Item *item) {
  Item_cond *cond = static_cast<Item_cond *>(item);
  DBUG_ASSERT(cond);

  if (pq_not_support_functype(cond->functype())) {
    return false;
  }

  Item *arg_item = nullptr;
  List_iterator_fast<Item> it(*cond->argument_list());
  for (size_t i = 0; (arg_item = it++); i++) {
    if (arg_item->type() == Item::SUM_FUNC_ITEM ||         //c1
        !check_pq_support_fieldtype(arg_item)) {             //c2
      return false;
    }
  }

  return true;
}

static bool check_pq_support_fieldtype_of_sum_func_item(Item *item) {
  Item_sum *sum = static_cast<Item_sum *>(item);
  if (!sum || pq_not_support_aggr_functype(sum->sum_func())) {
    return false;
  }

  for (uint i =0; i < sum->get_arg_count(); i++)
  {
    if (!check_pq_support_fieldtype(sum->get_arg(i))) {
      return false;
    }
  }

  return true;
}

static bool check_pq_support_fieldtype_of_ref_item(Item *item) {  
  Item_ref *item_ref = down_cast<Item_ref *>(item);
  if (!item_ref || pq_not_support_ref(item_ref)) {
    return false;
  }

  if (item_ref->ref[0]->type() == Item::SUM_FUNC_ITEM ||
      !check_pq_support_fieldtype(item_ref->ref[0])) {
    return false;
  }

  return true;
}

static bool check_pq_support_fieldtype_of_cache_item(Item *item) {
  Item_cache *item_cache = dynamic_cast<Item_cache*>(item);
  if (item_cache == nullptr) {
    return false;
  }

  Item *example_item = item_cache->get_example();
  if (!example_item || example_item->type() == Item::SUM_FUNC_ITEM ||         //c1
      !check_pq_support_fieldtype(example_item)) {            //c2
    return false;
  }

  return true;
}

static bool check_pq_support_fieldtype_of_row_item(Item *item) {
  // check each item in Item_row
  Item_row *row_item = down_cast<Item_row *>(item);
  for (uint i = 0; i < row_item->cols(); i++) {
    Item *n_item = row_item->element_index(i);
    if (!n_item || n_item->type() == Item::SUM_FUNC_ITEM ||         //c1
        !check_pq_support_fieldtype(n_item)) {             //c2
      return false;
    }
  }

  return true;
}

static PQ_CHECK_ITEM_TYPE g_check_item_type[] = {
  {Item::INVALID_ITEM, nullptr},
  {Item::FIELD_ITEM, check_pq_support_fieldtype_of_field_item},
  {Item::FUNC_ITEM, check_pq_support_fieldtype_of_func_item},
  {Item::SUM_FUNC_ITEM, check_pq_support_fieldtype_of_sum_func_item},
  {Item::STRING_ITEM, nullptr},
  {Item::INT_ITEM, nullptr},
  {Item::REAL_ITEM, nullptr},
  {Item::NULL_ITEM, nullptr},
  {Item::VARBIN_ITEM, nullptr},
  {Item::COPY_STR_ITEM, nullptr},
  {Item::FIELD_AVG_ITEM, nullptr},
  {Item::DEFAULT_VALUE_ITEM, nullptr},
  {Item::PROC_ITEM, nullptr},
  {Item::COND_ITEM, check_pq_support_fieldtype_of_cond_item},
  {Item::REF_ITEM, check_pq_support_fieldtype_of_ref_item},
  {Item::FIELD_STD_ITEM, nullptr},
  {Item::FIELD_VARIANCE_ITEM, nullptr},
  {Item::INSERT_VALUE_ITEM, nullptr},
  {Item::SUBSELECT_ITEM, nullptr},
  {Item::ROW_ITEM, check_pq_support_fieldtype_of_row_item},
  {Item::CACHE_ITEM, check_pq_support_fieldtype_of_cache_item},
  {Item::TYPE_HOLDER, nullptr},
  {Item::PARAM_ITEM, nullptr},
  {Item::TRIGGER_FIELD_ITEM, nullptr},
  {Item::DECIMAL_ITEM, nullptr},
  {Item::XPATH_NODESET, nullptr},
  {Item::XPATH_NODESET_CMP, nullptr},
  {Item::VIEW_FIXER_ITEM, nullptr},
  {Item::FIELD_BIT_ITEM, nullptr},
  {Item::NULL_RESULT_ITEM, nullptr},
  {Item::VALUES_COLUMN_ITEM, nullptr}
};

/**
 * check item is supported by Parallel Query or not
 *
 * @retval:
 *     true : supported
 *     false : not supported
 */
static bool check_pq_support_fieldtype(Item *item) {
  if (item == nullptr || pq_not_support_datatype(item->data_type())) {
    return false;
  }

  if (g_check_item_type[item->type()].fun_ptr != nullptr) {
    return g_check_item_type[item->type()].fun_ptr(item);
  }

  return true;
}

/*
 * check if order_list contains aggregate function
 *
 * @retval:
 *    true: contained
 *    false:
 */
static bool check_pq_sort_aggregation(const ORDER_with_src &order_list) {
  if (!order_list.order) {
    return false;
  }

  ORDER *tmp = nullptr;
  Item *order_item = nullptr;

  for (tmp = order_list.order; tmp; tmp = tmp->next) {
    order_item = *(tmp->item);
    if (!check_pq_support_fieldtype(order_item)) {
      return true;
    }
  }

  return false;
}

/*
 * generate item's result_field
 *
 * @retval:
 *    false: generate success
 *    ture: otherwise
 */
bool pq_create_result_fields(THD *thd, Temp_table_param *param,
                             List<Item> &fields, bool save_sum_fields,
                             ulonglong select_options, MEM_ROOT *root) {
  const bool not_all_columns = !(select_options & TMP_TABLE_ALL_COLUMNS);
  long hidden_field_count = param->hidden_field_count;
  Field *from_field = nullptr;
  Field **tmp_from_field = &from_field;
  Field **default_field = &from_field;

  bool force_copy_fields = false;
  TABLE_SHARE s;
  TABLE table;
  table.s = &s;

  uint copy_func_count = param->func_count;
  if (param->precomputed_group_by) {
    copy_func_count += param->sum_func_count;
  }

  Func_ptr_array *copy_func = new (root) Func_ptr_array(root);
  if (!copy_func) {
    return true;
  }

  copy_func->reserve(copy_func_count);
  List_iterator_fast<Item> li(fields);
  Item *item;
  while ((item = li++)) {
    Field *new_field = NULL;
    Item::Type type = item->type();
    const bool is_sum_func =
        type == Item::SUM_FUNC_ITEM && !item->m_is_window_function;

    if (type == Item::COPY_STR_ITEM) {
      item = ((Item_copy *)item)->get_item();
      if (item != nullptr) {
        type = item->type();
      }
    }

    if (not_all_columns && item != nullptr) {
      if (item->has_aggregation() && type != Item::SUM_FUNC_ITEM) {
        if (item->used_tables() & OUTER_REF_TABLE_BIT) {
          item->update_used_tables();
        }

        if (type == Item::SUBSELECT_ITEM ||
            (item->used_tables() & ~OUTER_REF_TABLE_BIT)) {
          param->using_outer_summary_function = 1;
          goto update_hidden;
        }
      }

      if (item->m_is_window_function) {
        if (!param->m_window || param->m_window_frame_buffer) {
          goto update_hidden;
        }

        if (param->m_window != down_cast<Item_sum *>(item)->window()) {
          goto update_hidden;
        }
      } else if (item->has_wf()) {
        if (param->m_window == nullptr || !param->m_window->is_last()) {
          goto update_hidden;
        }
      }
      if (item->const_item() && (int)hidden_field_count <= 0) {
        continue;  // We don't have to store this
      }
    }

    if (is_sum_func && !save_sum_fields) {
      /* Can't calc group yet */
    } else {
      if (param->schema_table) {
        new_field = item ? create_tmp_field_for_schema(item, &table, root) : nullptr;
      } else {
        new_field = item ? create_tmp_field(thd, &table, item, type, copy_func,
                             tmp_from_field, default_field, false,  //(1)
                             !force_copy_fields && not_all_columns,
                             item->marker == Item::MARKER_BIT ||
                             param->bit_fields_as_long,  //(2)
                             force_copy_fields, false, root) : nullptr;
      }

      if (!new_field) {
        DBUG_ASSERT(thd->is_fatal_error());
        return true;
      }

      if (not_all_columns && type == Item::SUM_FUNC_ITEM) {
        ((Item_sum *)item)->result_field = new_field;
      }
    }

    update_hidden:
    if (!--hidden_field_count) {
      param->hidden_field_count = 0;
    }
  }  // end of while ((item=li++)).

  List_iterator_fast<Item> it(fields);
  Field *result_field = nullptr;

  while ((item = it++)) {
    // c1: const_item will not produce field in the first rewritten table
    if (item->const_item() ||
        item->basic_const_item()) {
      continue;
    }

    // c2: check Item_copy. In the original execution plan, const_item will be
    // transformed into Item_copy in the rewritten-table's slice.
    if (item->type() == Item::COPY_STR_ITEM) {
      Item *orig_item = down_cast<Item_copy *>(item)->get_item();
      DBUG_ASSERT(orig_item);
      if (orig_item->const_item() ||
          orig_item->basic_const_item())
        continue;
    }
    // note that: the above item will not be pushed into worker

    result_field = item->get_result_field();
    if (result_field) {
      enum_field_types field_type = result_field->type();
      // c3: result_field contains unsupported data type
      if (pq_not_support_datatype(field_type)) {
        return true;
      }
    } else {
      // c4: item is not FIELD_ITEM and it has no result_field
      if (item->type() != Item::FIELD_ITEM) {
        return true;
      }

      result_field = down_cast<Item_field *>(item)->result_field;
      if (result_field && pq_not_support_datatype(result_field->type())) {
        return true;
      }
    }
  }

  return false;
}

/**
 * check whether the select result fields is suitable for parallel query
 *
 * @return:
 *    true, suitable
 *    false.
 */
bool check_pq_select_result_fields(JOIN *join) {
  DBUG_ENTER("check result fields is suitable for parallel query or not");
  MEM_ROOT *pq_check_root = ::new MEM_ROOT();
  if (pq_check_root == nullptr) {
	DBUG_RETURN(false);
  }
  
  init_sql_alloc(key_memory_thd_main_mem_root, pq_check_root,
                 global_system_variables.query_alloc_block_size,
                 global_system_variables.query_prealloc_size);

  bool suit_for_parallel = false;

  bool base_slice = (join->last_slice_before_pq == REF_SLICE_SAVED_BASE);
  List<Item> tmp_all_fields = base_slice ? join->all_fields
                                         : join->tmp_all_fields0[join->last_slice_before_pq];
  List<Item> tmp_field_lists = base_slice ? join->fields_list
                                          : join->tmp_fields_list0[join->last_slice_before_pq];

  join->tmp_table_param->pq_copy(join->saved_tmp_table_param);
  join->tmp_table_param->copy_fields.clear();

  Temp_table_param *tmp_param =
          new (pq_check_root) Temp_table_param(*join->tmp_table_param);

  if (tmp_param == nullptr) {
    // free the memory
    free_root(pq_check_root, MYF(0));
    if (pq_check_root) ::delete pq_check_root;
    DBUG_RETURN(suit_for_parallel);
  }

  tmp_param->m_window_frame_buffer = true;
  List<Item> tmplist(tmp_all_fields, join->thd->mem_root); 
  tmp_param->hidden_field_count =
          tmp_all_fields.elements - tmp_field_lists.elements;

  //create_tmp_table may change the original item's result_field, hence
  //we must save it before.
  std::vector<Field *> saved_result_field (tmplist.size(), nullptr);
  List_iterator_fast<Item> it(tmp_all_fields);
  Item *tmp_item = nullptr;
  int i;

  for (i = 0, tmp_item = it++; tmp_item; i++, tmp_item = it++) {
    if (tmp_item->type() == Item::FIELD_ITEM || tmp_item->type() == Item::DEFAULT_VALUE_ITEM) {
      saved_result_field[i] = down_cast<Item_field *>(tmp_item)->result_field;
    } else {
      saved_result_field[i] = tmp_item->get_result_field();
    }
  }

  if (pq_create_result_fields(join->thd, tmp_param, tmplist, true,
          join->select_lex->active_options(), pq_check_root)) {
    suit_for_parallel = false;
  } else {
    suit_for_parallel = true;
  }

  //restore result_field
  it.rewind();

  for (i = 0, tmp_item = it++; tmp_item; i++, tmp_item = it++) {
    if (tmp_item->type() == Item::FIELD_ITEM || tmp_item->type() == Item::DEFAULT_VALUE_ITEM) {
      down_cast<Item_field *>(tmp_item)->result_field = saved_result_field[i];
    } else {
      tmp_item->set_result_field(saved_result_field[i]);
    }
  }

  // free the memory
  free_root(pq_check_root, MYF(0));
  if (pq_check_root) ::delete pq_check_root;
  DBUG_RETURN(suit_for_parallel);
}

/**
 * check whether the select fields is suitable for parallel query
 *
 * @return:
 *    true, suitable
 *    false.
 */
bool check_pq_select_fields(JOIN *join) {
   // check whether contains blob, text, json and geometry field
  List_iterator_fast<Item> it(join->all_fields);
  Item *item = nullptr;
  while ((item = it++)) {
    if (!check_pq_support_fieldtype(item)) {
      return false;
	  }
  }

  Item *n_where_cond = join->select_lex->where_cond();
  Item *n_having_cond = join->select_lex->having_cond();

  if (n_where_cond && !check_pq_support_fieldtype(n_where_cond)) {
    return false;
  }

  /*
   * For Having Aggr. function, the having_item will be pushed
   * into all_fields in prepare phase. Currently, we have not support this operation.
   */
  if (n_having_cond && !check_pq_support_fieldtype(n_having_cond)) {
    return false;
  }
  
  if (check_pq_sort_aggregation(join->order)) {
    return false;
  }

  if (!check_pq_select_result_fields(join)) {
	  return false;
  }
  
  return true;
}

/**
 * choose a table that do parallel query, currently only do parallel scan on
 * first no-const primary table.
 *
 *
 * @return:
 *    true, found a parallel scan table
 *    false, cann't found a parallel scan table
 */
bool choose_parallel_scan_table(JOIN *join) {
  QEP_TAB *tab = &join->qep_tab[join->const_tables];
  // only support table/index full/range scan
  join_type scan_type= tab->type();
  if (scan_type != JT_ALL &&
      scan_type != JT_INDEX_SCAN &&
      scan_type != JT_REF &&
      (scan_type != JT_RANGE || !tab->quick() ||
       tab->quick()->quick_select_type() != PQ_RANGE_SELECT)) {
    return false;
  }
  
  tab->do_parallel_scan = true;

  return true;
}

void set_pq_dop(THD *thd) {
  if (!thd->no_pq && thd->variables.force_parallel_execute && thd->pq_dop == 0) {
    thd->pq_dop = thd->variables.parallel_default_dop;
  }
}

/**
*  check whether  the parallel query is enabled and set the
*  parallel query condition status
*
*/
void set_pq_condition_status(THD *thd) {
  set_pq_dop(thd);
	
  if (thd->pq_dop > 0) {
    thd->m_suite_for_pq = PqConditionStatus::ENABLED;
  } else {
    thd->m_suite_for_pq = PqConditionStatus::NOT_SUPPORTED;
  }
}

bool suite_for_parallel_query(THD *thd) {
  if (thd->in_sp_trigger  ||                   // store procedure or trigger
      thd->m_attachable_trx   ||               // attachable transaction
      thd->tx_isolation == ISO_SERIALIZABLE)   // serializable without snapshot read
  {
    return false;
  }

  return true;
}

bool suite_for_parallel_query(LEX *lex) {
  if (lex->in_execute_ps){
	  return false;
  }
  
  return true;
}
	
bool suite_for_parallel_query(SELECT_LEX_UNIT *unit) {
  if (!unit->is_simple()){
	  return false;
  }
  
  return true;
}

bool suite_for_parallel_query(TABLE_LIST *tbl_list) {
  if (tbl_list->is_view()) {
    return false;
  }

  // skip explicit table lock
  if (tbl_list->lock_descriptor().type > TL_READ ||
      current_thd->locking_clause) {
    return false;
  }

  TABLE *tb = tbl_list->table;
  if (tb != nullptr &&
      (tb->s->tmp_table != NO_TMP_TABLE ||         // template table
        tb->file->ht->db_type != DB_TYPE_INNODB ||  // Non-InnoDB table
        tb->part_info ||                            // partition table
        tb->fulltext_searched)) {                     // fulltext match search
    return false;
	}
  return true; 
}

bool suite_for_parallel_query(SELECT_LEX *select) {
  if (select->first_inner_unit() != nullptr ||     // nesting subquery, including view〝derived table〝subquery condition and so on.
      select->outer_select() != nullptr ||         // nested subquery
      select->is_distinct() ||                     // select distinct
      select->saved_windows_elements)              // windows function
  {
    return false;
  }
  
  for (TABLE_LIST *tbl_list = select->table_list.first; tbl_list != nullptr;
       tbl_list = tbl_list->next_local) {
    if (!suite_for_parallel_query(tbl_list)) {
      return false;   
    }
  }
  
  for (TABLE_LIST *tbl_list = select->leaf_tables; tbl_list != nullptr;
       tbl_list = tbl_list->next_leaf) {
    if (!suite_for_parallel_query(tbl_list)) {
      return false;   
    }
  }
  return true;
}

bool suite_for_parallel_query(JOIN *join) {
  if ((join->best_read < join->thd->variables.parallel_cost_threshold) ||
    (join->primary_tables == join->const_tables/* || primary_tables > 1*/) ||
    (join->select_distinct || join->select_count)  ||
    (join->all_fields.elements > MAX_FIELDS) ||
    (join->rollup.state != ROLLUP::State::STATE_NONE) ||
    (join->zero_result_cause != nullptr)) {
    return false;
  }
	
  if (!check_pq_select_fields(join)) {
	  return false;
  }

  return true;  
}

static bool check_pq_running_threads(uint dop, ulong timeout_ms) {
  bool success = false;
  mysql_mutex_lock(&LOCK_pq_threads_running);
  if (parallel_threads_running + dop > parallel_max_threads) {
    if (timeout_ms > 0) {
      struct timespec start_ts;
      struct timespec end_ts;
      struct timespec abstime;
      ulong wait_timeout = timeout_ms;
      int wait_result;

      start:
      set_timespec(&start_ts, 0);
      /* Calcuate the waiting period. */
      abstime.tv_sec = start_ts.tv_sec + wait_timeout / TIME_THOUSAND;
      abstime.tv_nsec =
          start_ts.tv_nsec + (wait_timeout % TIME_THOUSAND) * TIME_MILLION;
      if (abstime.tv_nsec >= TIME_BILLION) {
        abstime.tv_sec++;
        abstime.tv_nsec -= TIME_BILLION;
      }
      wait_result = mysql_cond_timedwait(&COND_pq_threads_running,
                                         &LOCK_pq_threads_running, &abstime);
      if (parallel_threads_running + dop <= parallel_max_threads) {
        success = true;
      } else {
        success = false;
        if (!wait_result) {  // wait isn't timeout
          set_timespec(&end_ts, 0);
          ulong difftime = (end_ts.tv_sec - start_ts.tv_sec) * TIME_THOUSAND +
                           (end_ts.tv_nsec - start_ts.tv_nsec) / TIME_MILLION;
          wait_timeout -= difftime;
          goto start;
        }
      }
    }
  } else {
    success = true;
  }
  
  if (success) {
    parallel_threads_running += dop;
    current_thd->pq_threads_running += dop;
  }
  
  mysql_mutex_unlock(&LOCK_pq_threads_running);
  return success;
}

bool check_pq_conditions(THD *thd) {
  // max PQ memory size limit
  if (get_pq_memory_total() >= parallel_memory_limit) {
    atomic_add<uint>(parallel_memory_refused, 1);
    return false;
  }

  // max PQ threads limit
  if (!check_pq_running_threads(thd->pq_dop,
      thd->variables.parallel_queue_timeout)) {
    atomic_add<uint>(parallel_threads_refused, 1);
    return false;
  }

  // RBO limit
  if (!suite_for_parallel_query(thd)) {
    return false;
  }
  
  if (!suite_for_parallel_query(thd->lex)) {
    return false;
  }
  
  if (!suite_for_parallel_query(thd->lex->unit)) {
    return false;
  }
  
  SELECT_LEX *select = thd->lex->unit->first_select();
  if (!suite_for_parallel_query(select)) {
    return false;
  }
  
  if (!suite_for_parallel_query(select->join)) {
    return false;
  }
  
  if (!choose_parallel_scan_table(select->join)) {
    return false;
  }
  
  return true;
}
