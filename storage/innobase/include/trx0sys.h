/*****************************************************************************

Copyright (c) 1996, 2019, Oracle and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/** @file include/trx0sys.h
 Transaction system

 Created 3/26/1996 Heikki Tuuri
 *******************************************************/

#ifndef trx0sys_h
#define trx0sys_h

#include "univ.i"

#include "buf0buf.h"
#include "fil0fil.h"
#include "trx0types.h"
#ifndef UNIV_HOTBACKUP
#include "mem0mem.h"
#include "mtr0mtr.h"
#include "page0types.h"
#include "ut0byte.h"
#include "ut0lst.h"
#include "ut0mutex.h"
#endif /* !UNIV_HOTBACKUP */
#include <atomic>
#include <vector>
#include "trx0trx.h"
#include "lf.h"

#ifndef UNIV_HOTBACKUP
typedef UT_LIST_BASE_NODE_T(trx_t) trx_ut_list_t;

// Forward declaration
class MVCC;
class ReadView;

/** The transaction system */
extern trx_sys_t *trx_sys;

trx_t *current_trx();

struct rw_trx_hash_element_t {
  rw_trx_hash_element_t() : id(0), trx(nullptr) {
    mutex_create(LATCH_ID_RW_TRX_HASH_ELEMENT, &mutex);
  }

  ~rw_trx_hash_element_t() { mutex_free(&mutex); }

  trx_id_t id; /* lf_hash_init() relies on this to be first in the struct */
  trx_t *trx;
  ib_mutex_t mutex;
};

/**
  Wrapper around LF_HASH to store set of in memory read-write transactions.
*/

class rw_trx_hash_t {
  LF_HASH hash;

  /**
    Constructor callback for lock-free allocator.

    Object is just allocated and is not yet accessible via rw_trx_hash by
    concurrent threads. Object can be reused multiple times before it is freed.
    Every time object is being reused initializer() callback is called.
  */

  static void rw_trx_hash_constructor(uchar *arg) {
    new (arg + LF_HASH_OVERHEAD) rw_trx_hash_element_t();
  }

  /**
    Destructor callback for lock-free allocator.

    Object is about to be freed and is not accessible via rw_trx_hash by
    concurrent threads.
  */

  static void rw_trx_hash_destructor(uchar *arg) {
    reinterpret_cast<rw_trx_hash_element_t *>(arg + LF_HASH_OVERHEAD)
        ->~rw_trx_hash_element_t();
  }

  /**
    Initializer callback for lock-free hash.

    Object is not yet accessible via rw_trx_hash by concurrent threads, but is
    about to become such. Object id can be changed only by this callback and
    remains the same until all pins to this object are released.

    Object trx can be changed to 0 by erase() under object mutex protection,
    which indicates it is about to be removed from lock-free hash and become
    not accessible by concurrent threads.
  */

  static void rw_trx_hash_initializer(rw_trx_hash_element_t *element,
                                      trx_t *trx) {
    element->trx = trx;
    element->id = trx->id;
    trx->rw_trx_hash_element = element;
  }

  /**
    Gets LF_HASH pins.

    Pins are used to protect object from being destroyed or reused. They are
    normally stored in trx object for quick access. If caller doesn't have trx
    available, we try to get it using currnet_trx(). If caller doesn't have trx
    at all, temporary pins are allocated.
  */

  LF_PINS *get_pins(trx_t *trx) {
    if (!trx->rw_trx_hash_pins) {
      trx->rw_trx_hash_pins = lf_hash_get_pins(&hash);
      ut_a(trx->rw_trx_hash_pins != nullptr);
    }
    return trx->rw_trx_hash_pins;
  }

 public:
  void init() {
    lf_hash_init(&hash, sizeof(rw_trx_hash_element_t), LF_HASH_UNIQUE, 0,
                 sizeof(trx_id_t), nullptr, &my_charset_bin);
    hash.alloc.constructor = rw_trx_hash_constructor;
    hash.alloc.destructor = rw_trx_hash_destructor;
    hash.initialize =
        reinterpret_cast<lf_hash_init_func *>(rw_trx_hash_initializer);
  }

  void destroy() { lf_hash_destroy(&hash); }

  /**
    Releases LF_HASH pins.

    Must be called by thread that owns trx_t object when the latter is being
    "detached" from thread (e.g. released to the pool by trx_free()). Can be
    called earlier if thread is expected not to use rw_trx_hash.

    Since pins are not allowed to be transferred to another thread,
    initialisation thread calls this for recovered transactions.
  */

  void put_pins(trx_t *trx) {
    if (trx->rw_trx_hash_pins) {
      lf_hash_put_pins(trx->rw_trx_hash_pins);
      trx->rw_trx_hash_pins = nullptr;
    }
  }

#ifdef UNIV_DEBUG
  static void validate_element(trx_t *trx) {
    mutex_enter(&trx->mutex);
    ut_ad(trx_state_eq(trx, TRX_STATE_ACTIVE) ||
          trx_state_eq(trx, TRX_STATE_PREPARED));
    mutex_exit(&trx->mutex);
  }
#endif // UNIV_DEBUG

  /**
    Finds trx object in lock-free hash with given id.

    Only ACTIVE or PREPARED trx objects may participate in hash. Nevertheless
    the transaction may get committed before this method returns.

    With do_ref_count == false the caller may dereference returned trx pointer
    only if lock_sys->mutex was acquired before calling find().

    With do_ref_count == true caller may dereference trx even if it is not
    holding lock_sys->mutex. Caller is responsible for calling
    trx_release_reference() when it is done playing with trx.

    Ideally this method should get caller rw_trx_hash_pins along with trx
    object as a parameter, similar to insert() and erase(). However most
    callers lose trx early in their call chains and it is not that easy to pass
    them through.

    So we take more expensive approach: get trx through current_thd()->ha_data.
    Some threads don't have trx attached to THD, and at least server
    initialisation thread, fts_optimize_thread, srv_master_thread,
    dict_stats_thread, srv_monitor_thread, btr_defragment_thread don't even
    have THD at all. For such cases we allocate pins only for duration of
    search and free them immediately.

    This has negative performance impact and should be fixed eventually (by
    passing caller_trx as a parameter). Still stream of DML is more or less Ok.

    @return
      @retval 0 not found
      @retval pointer to trx
  */

  trx_t *find(trx_t *caller_trx, trx_id_t trx_id, bool do_ref_count = false) {
    if (!trx_id) {
      return nullptr;
    }

    trx_t *trx = nullptr;
    LF_PINS *pins = caller_trx ? get_pins(caller_trx) : lf_hash_get_pins(&hash);
    ut_a(pins != nullptr);

    auto element = reinterpret_cast<rw_trx_hash_element_t *>(
        lf_hash_search(&hash, pins, reinterpret_cast<const void *>(&trx_id),
                       sizeof(trx_id_t)));
    if (element) {
      mutex_enter(&element->mutex);
      lf_hash_search_unpin(pins);
      if ((trx = element->trx)) {
        ut_d(validate_element(trx));
        if (do_ref_count) {
          trx->reference();
        }
      }
      mutex_exit(&element->mutex);
    }
    else {
      lf_hash_search_unpin(pins);
    }
    if (!caller_trx) {
      lf_hash_put_pins(pins);
    }
    return trx;
  }

  trx_t *find(trx_id_t trx_id, bool do_ref_count = false) {
    return find(current_trx(), trx_id, do_ref_count);
  }

  /**
    Inserts trx to lock-free hash.

    Object becomes accessible via rw_trx_hash.
  */

  void insert(trx_t *trx) {
    ut_d(validate_element(trx));
    int res =
        lf_hash_insert(&hash, get_pins(trx), reinterpret_cast<void *>(trx));
    ut_a(res == 0);
  }

  /**
    Removes trx from lock-free hash.

    Object becomes not accessible via rw_trx_hash. But it still can be pinned
    by concurrent find(), which is supposed to release it immediately after
    it sees object trx is 0.
  */

  void erase(trx_t *trx) {
    ut_d(validate_element(trx));
    mutex_enter(&trx->rw_trx_hash_element->mutex);
    trx->rw_trx_hash_element->trx = nullptr;
    mutex_exit(&trx->rw_trx_hash_element->mutex);
    int res = lf_hash_delete(&hash, get_pins(trx),
                             reinterpret_cast<const void *>(&trx->id),
                             sizeof(trx_id_t));
    ut_a(res == 0);
  }

  /**
    Returns the number of elements in the hash.

    The number is exact only if hash is protected against concurrent
    modifications (e.g. single threaded startup or hash is protected
    by some mutex). Otherwise the number may be used as a hint only,
    because it may change even before this method returns.
  */

  int32_t size() { return hash.count.load(std::memory_order_relaxed); }
};

/** Checks if a page address is the trx sys header page.
@param[in]	page_id	page id
@return true if trx sys header page */
UNIV_INLINE
bool trx_sys_hdr_page(const page_id_t &page_id);

/** Creates and initializes the central memory structures for the transaction
 system. This is called when the database is started.
 @return min binary heap of rsegs to purge */
purge_pq_t *trx_sys_init_at_db_start(void);
/** Creates the trx_sys instance and initializes purge_queue and mutex. */
void trx_sys_create(void);
/** Creates and initializes the transaction system at the database creation. */
void trx_sys_create_sys_pages(void);

/** Find the page number in the TRX_SYS page for a given slot/rseg_id
@param[in]	rseg_id		slot number in the TRX_SYS page rseg array
@return page number from the TRX_SYS page rseg array */
page_no_t trx_sysf_rseg_find_page_no(ulint rseg_id);

/** Look for a free slot for a rollback segment in the trx system file copy.
@param[in,out]	mtr		mtr
@return slot index or ULINT_UNDEFINED if not found */
ulint trx_sysf_rseg_find_free(mtr_t *mtr);

/** Gets a pointer to the transaction system file copy and x-locks its page.
 @return pointer to system file copy, page x-locked */
UNIV_INLINE
trx_sysf_t *trx_sysf_get(mtr_t *mtr); /*!< in: mtr */

/** Gets the space of the nth rollback segment slot in the trx system
file copy.
@param[in]	sys_header	trx sys file copy
@param[in]	i		slot index == rseg id
@param[in]	mtr		mtr
@return space id */
UNIV_INLINE
space_id_t trx_sysf_rseg_get_space(trx_sysf_t *sys_header, ulint i, mtr_t *mtr);

/** Gets the page number of the nth rollback segment slot in the trx system
file copy.
@param[in]	sys_header	trx sys file copy
@param[in]	i		slot index == rseg id
@param[in]	mtr		mtr
@return page number, FIL_NULL if slot unused */
UNIV_INLINE
page_no_t trx_sysf_rseg_get_page_no(trx_sysf_t *sys_header, ulint i,
                                    mtr_t *mtr);

/** Sets the space id of the nth rollback segment slot in the trx system
file copy.
@param[in]	sys_header	trx sys file copy
@param[in]	i		slot index == rseg id
@param[in]	space		space id
@param[in]	mtr		mtr */
UNIV_INLINE
void trx_sysf_rseg_set_space(trx_sysf_t *sys_header, ulint i, space_id_t space,
                             mtr_t *mtr);

/** Set the page number of the nth rollback segment slot in the trx system
file copy.
@param[in]	sys_header	trx sys file copy
@param[in]	i		slot index == rseg id
@param[in]	page_no		page number, FIL_NULL if the slot is reset to
                                unused
@param[in]	mtr		mtr */
UNIV_INLINE
void trx_sysf_rseg_set_page_no(trx_sysf_t *sys_header, ulint i,
                               page_no_t page_no, mtr_t *mtr);

/** Allocates a new transaction id.
 @return new, allocated trx id */
UNIV_INLINE
trx_id_t trx_sys_get_new_trx_id();
/** Determines the maximum transaction id.
 @return maximum currently allocated trx id; will be stale after the
 next call to trx_sys_get_new_trx_id() */
UNIV_INLINE
trx_id_t trx_sys_get_max_trx_id(void);

#ifdef UNIV_DEBUG
/* Flag to control TRX_RSEG_N_SLOTS behavior debugging. */
extern uint trx_rseg_n_slots_debug;
#endif
#endif /* !UNIV_HOTBACKUP */

/** Writes a trx id to an index page. In case that the id size changes in some
future version, this function should be used instead of mach_write_...
@param[in]	ptr	pointer to memory where written
@param[in]	id	id */
UNIV_INLINE
void trx_write_trx_id(byte *ptr, trx_id_t id);

#ifndef UNIV_HOTBACKUP
/** Reads a trx id from an index page. In case that the id size changes in
 some future version, this function should be used instead of
 mach_read_...
 @return id */
UNIV_INLINE
trx_id_t trx_read_trx_id(
    const byte *ptr); /*!< in: pointer to memory from where to read */

/** Returns the minimum trx id in rw trx list. This is the smallest id for which
 the trx can possibly be active. (But, you must look at the trx->state to
 find out if the minimum trx id transaction itself is active, or already
 committed.)
 @return the minimum trx id, or trx_sys->max_trx_id if the trx list is empty */
UNIV_INLINE
trx_id_t trx_rw_min_trx_id(void);

/** Persist transaction number limit below which all transaction GTIDs
are persisted to disk table.
@param[in]	gtid_trx_no	transaction number */
void trx_sys_persist_gtid_num(trx_id_t gtid_trx_no);

/** @return oldest transaction number yet to be committed. */
trx_id_t trx_sys_oldest_trx_no();

/** Get a list of all binlog prepared transactions.
@param[out]	trx_ids	all prepared transaction IDs. */
void trx_sys_get_binlog_prepared(std::vector<trx_id_t> &trx_ids);

/** Get current binary log positions stored.
@param[out]	file	binary log file name
@param[out]	offset	binary log file offset */
void trx_sys_read_binlog_position(char *file, uint64_t &offset);

/** Update binary log position if not already updated. This is called
by clone to update any stale binary log position if any transaction
is yet to update the binary log position in SE.
@param[in]	last_file	last noted binary log file name
@param[in]	last_offset	last noted binary log offset
@param[in]	file		current binary log file name
@param[in]	offset		current binary log file offset
@return true, if binary log position is updated with current. */
bool trx_sys_write_binlog_position(const char *last_file, uint64_t last_offset,
                                   const char *file, uint64_t offset);

/** Updates the offset information about the end of the MySQL binlog entry
which corresponds to the transaction being committed, external XA transaction
being prepared or rolled back. In a MySQL replication slave updates the latest
master binlog position up to which replication has proceeded.
@param[in]	trx	current transaction
@param[in,out]	mtr	mini transaction for update */
void trx_sys_update_mysql_binlog_offset(trx_t *trx, mtr_t *mtr);

/** Shutdown/Close the transaction system. */
void trx_sys_close(void);

/** Determine if there are incomplete transactions in the system.
@return whether incomplete transactions need rollback */
UNIV_INLINE
bool trx_sys_need_rollback();

/*********************************************************************
Check if there are any active (non-prepared) transactions.
@return total number of active transactions or 0 if none */
ulint trx_sys_any_active_transactions(void);
#endif /* !UNIV_HOTBACKUP */

#ifdef UNIV_DEBUG
/** Validate the trx_sys_t::rw_trx_list.
 @return true if the list is valid */
bool trx_sys_validate_trx_list();
#endif /* UNIV_DEBUG */

/** Initialize trx_sys_undo_spaces, called once during srv_start(). */
void trx_sys_undo_spaces_init();

/** Free the resources occupied by trx_sys_undo_spaces,
called once during thread de-initialization. */
void trx_sys_undo_spaces_deinit();

/** The automatically created system rollback segment has this id */
#define TRX_SYS_SYSTEM_RSEG_ID 0

/** The offset of the transaction system header on the page */
#define TRX_SYS FSEG_PAGE_DATA

/** Transaction system header */
/*------------------------------------------------------------- @{ */
#define TRX_SYS_TRX_ID_STORE       \
  0 /*!< the maximum trx id or trx \
    number modulo                  \
    TRX_SYS_TRX_ID_UPDATE_MARGIN   \
    written to a file page by any  \
    transaction; the assignment of \
    transaction ids continues from \
    this number rounded up by      \
    TRX_SYS_TRX_ID_UPDATE_MARGIN   \
    plus                           \
    TRX_SYS_TRX_ID_UPDATE_MARGIN   \
    when the database is           \
    started */
#define TRX_SYS_FSEG_HEADER     \
  8 /*!< segment header for the \
    tablespace segment the trx  \
    system is created into */
#define TRX_SYS_RSEGS (8 + FSEG_HEADER_SIZE)
/*!< the start of the array of
rollback segment specification
slots */
/*------------------------------------------------------------- @} */

/* Originally, InnoDB defined TRX_SYS_N_RSEGS as 256 but created only one
rollback segment.  It initialized some arrays with this number of entries.
We must remember this limit in order to keep file compatibility. */
#define TRX_SYS_OLD_N_RSEGS 256

/* The system temporary tablespace was originally allocated rseg_id slot
numbers 1 through 32 in the TRX_SYS page.  But those slots were not used
because those Rollback segments were recreated at startup and after any
crash. These slots are now used for redo-enabled rollback segments.
The default number of rollback segments in the temporary tablespace
remains the same. */
#define TRX_SYS_OLD_TMP_RSEGS 32

/** Maximum length of MySQL binlog file name, in bytes. */
#define TRX_SYS_MYSQL_LOG_NAME_LEN 512
/** Contents of TRX_SYS_MYSQL_LOG_MAGIC_N_FLD */
#define TRX_SYS_MYSQL_LOG_MAGIC_N 873422344

#if UNIV_PAGE_SIZE_MIN < 4096
#error "UNIV_PAGE_SIZE_MIN < 4096"
#endif
/** The offset of the MySQL binlog offset info in the trx system header */
#define TRX_SYS_MYSQL_LOG_INFO (UNIV_PAGE_SIZE - 1000)
#define TRX_SYS_MYSQL_LOG_MAGIC_N_FLD \
  0 /*!< magic number which is        \
    TRX_SYS_MYSQL_LOG_MAGIC_N         \
    if we have valid data in the      \
    MySQL binlog info */
#define TRX_SYS_MYSQL_LOG_OFFSET_HIGH \
  4 /*!< high 4 bytes of the offset   \
    within that file */
#define TRX_SYS_MYSQL_LOG_OFFSET_LOW                             \
  8                               /*!< low 4 bytes of the offset \
                                  within that file */
#define TRX_SYS_MYSQL_LOG_NAME 12 /*!< MySQL log file name */

/** Reserve next 8 bytes for transaction number up to which GTIDs
are persisted to table */
#define TRX_SYS_TRX_NUM_GTID \
  (TRX_SYS_MYSQL_LOG_INFO + TRX_SYS_MYSQL_LOG_NAME + TRX_SYS_MYSQL_LOG_NAME_LEN)
#define TRX_SYS_TRX_NUM_END = (TRX_SYS_TRX_NUM_GTID + 8)

/** Doublewrite buffer */
/* @{ */
/** The offset of the doublewrite buffer header on the trx system header page */
#define TRX_SYS_DOUBLEWRITE (UNIV_PAGE_SIZE - 200)
/*-------------------------------------------------------------*/
#define TRX_SYS_DOUBLEWRITE_FSEG \
  0 /*!< fseg header of the fseg \
    containing the doublewrite   \
    buffer */
#define TRX_SYS_DOUBLEWRITE_MAGIC FSEG_HEADER_SIZE
/*!< 4-byte magic number which
shows if we already have
created the doublewrite
buffer */
#define TRX_SYS_DOUBLEWRITE_BLOCK1 (4 + FSEG_HEADER_SIZE)
/*!< page number of the
first page in the first
sequence of 64
(= FSP_EXTENT_SIZE) consecutive
pages in the doublewrite
buffer */
#define TRX_SYS_DOUBLEWRITE_BLOCK2 (8 + FSEG_HEADER_SIZE)
/*!< page number of the
first page in the second
sequence of 64 consecutive
pages in the doublewrite
buffer */
#define TRX_SYS_DOUBLEWRITE_REPEAT \
  12 /*!< we repeat                \
     TRX_SYS_DOUBLEWRITE_MAGIC,    \
     TRX_SYS_DOUBLEWRITE_BLOCK1,   \
     TRX_SYS_DOUBLEWRITE_BLOCK2    \
     so that if the trx sys        \
     header is half-written        \
     to disk, we still may         \
     be able to recover the        \
     information */
/** If this is not yet set to TRX_SYS_DOUBLEWRITE_SPACE_ID_STORED_N,
we must reset the doublewrite buffer, because starting from 4.1.x the
space id of a data page is stored into
FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID. */
#define TRX_SYS_DOUBLEWRITE_SPACE_ID_STORED (24 + FSEG_HEADER_SIZE)

/*-------------------------------------------------------------*/
/** Contents of TRX_SYS_DOUBLEWRITE_MAGIC */
#define TRX_SYS_DOUBLEWRITE_MAGIC_N 536853855
/** Contents of TRX_SYS_DOUBLEWRITE_SPACE_ID_STORED */
#define TRX_SYS_DOUBLEWRITE_SPACE_ID_STORED_N 1783657386

/** Size of the doublewrite block in pages */
#define TRX_SYS_DOUBLEWRITE_BLOCK_SIZE FSP_EXTENT_SIZE
/* @} */

/** List of undo tablespace IDs. */
class Space_Ids : public std::vector<space_id_t, ut_allocator<space_id_t>> {
 public:
  void sort() { std::sort(begin(), end()); }

  bool contains(space_id_t id) {
    if (size() == 0) {
      return (false);
    }

    iterator it = std::find(begin(), end(), id);

    return (it != end());
  }

  iterator find(space_id_t id) { return (std::find(begin(), end(), id)); }
};

#ifndef UNIV_HOTBACKUP
/** The transaction system central memory data structure. */
struct trx_sys_t {
  TrxSysMutex mutex; /*!< mutex protecting most fields in
                     this structure except when noted
                     otherwise */

  MVCC *mvcc;                   /*!< Multi version concurrency control
                                manager */
  volatile trx_id_t max_trx_id; /*!< The smallest number not yet
                                assigned as a transaction id or
                                transaction number. This is declared
                                volatile because it can be accessed
                                without holding any mutex during
                                AC-NL-RO view creation. */
  std::atomic<trx_id_t> min_active_id;
  /*!< Minimal transaction id which is
  still in active state. */
  trx_ut_list_t serialisation_list;
  /*!< Ordered on trx_t::no of all the
  currenrtly active RW transactions */
#ifdef UNIV_DEBUG
  trx_id_t rw_max_trx_no; /*!< Max trx number of read-write
                          transactions added for purge. */
#endif                    /* UNIV_DEBUG */

  char pad1[ut::INNODB_CACHE_LINE_SIZE];             /*!< To avoid false sharing */
  trx_ut_list_t rw_trx_list; /*!< List of active and committed in
                             memory read-write transactions, sorted
                             on trx id, biggest first. Recovered
                             transactions are always on this list. */

  char pad2[ut::INNODB_CACHE_LINE_SIZE];                /*!< To avoid false sharing */
  trx_ut_list_t mysql_trx_list; /*!< List of transactions created
                                for MySQL. All user transactions are
                                on mysql_trx_list. The rw_trx_list
                                can contain system transactions and
                                recovered transactions that will not
                                be in the mysql_trx_list.
                                mysql_trx_list may additionally contain
                                transactions that have not yet been
                                started in InnoDB. */

  trx_ids_t rw_trx_ids; /*!< Array of Read write transaction IDs
                        for MVCC snapshot. A ReadView would take
                        a snapshot of these transactions whose
                        changes are not visible to it. We should
                        remove transactions from the list before
                        committing in memory and releasing locks
                        to ensure right order of removal and
                        consistent snapshot. */

  char pad3[ut::INNODB_CACHE_LINE_SIZE]; /*!< To avoid false sharing */

  Rsegs rsegs; /*!< Vector of pointers to rollback
               segments. These rsegs are iterated
               and added to the end under a read
               lock. They are deleted under a write
               lock while the vector is adjusted.
               They are created and destroyed in
               single-threaded mode. */

  Rsegs tmp_rsegs; /*!< Vector of pointers to rollback
                   segments within the temp tablespace;
                   This vector is created and destroyed
                   in single-threaded mode so it is not
                   protected by any mutex because it is
                   read-only during multi-threaded
                   operation. */

  ulint rseg_history_len;
  /*!< Length of the TRX_RSEG_HISTORY
  list (update undo logs for committed
  transactions), protected by
  rseg->mutex */

  const char rw_trx_hash_pre_pad[ut::INNODB_CACHE_LINE_SIZE];

  /**
    Lock-free hash of in memory read-write transactions.
  */
  rw_trx_hash_t rw_trx_hash;

  const char rw_trx_hash_post_pad[ut::INNODB_CACHE_LINE_SIZE];

  ulint n_prepared_trx; /*!< Number of transactions currently
                        in the XA PREPARED state */

  bool found_prepared_trx; /*!< True if XA PREPARED trxs are
                           found. */
};

#endif /* !UNIV_HOTBACKUP */

/** A list of undo tablespace IDs found in the TRX_SYS page.
This cannot be part of the trx_sys_t object because it is initialized before
that object is created. These are the old type of undo tablespaces that do not
have space_IDs in the reserved range nor contain an RSEG_ARRAY page. */
extern Space_Ids *trx_sys_undo_spaces;

/** When a trx id which is zero modulo this number (which must be a power of
two) is assigned, the field TRX_SYS_TRX_ID_STORE on the transaction system
page is updated */
#define TRX_SYS_TRX_ID_WRITE_MARGIN ((trx_id_t)256)

/** Test if trx_sys->mutex is owned. */
#define trx_sys_mutex_own() (trx_sys->mutex.is_owned())

/** Acquire the trx_sys->mutex. */
#define trx_sys_mutex_enter()     \
  do {                            \
    mutex_enter(&trx_sys->mutex); \
  } while (0)

/** Release the trx_sys->mutex. */
#define trx_sys_mutex_exit() \
  do {                       \
    trx_sys->mutex.exit();   \
  } while (0)

#include "trx0sys.ic"

#endif
