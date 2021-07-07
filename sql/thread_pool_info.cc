/* Copyright(C) 2019 MariaDB

This program is free software; you can redistribute itand /or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02111 - 1301 USA*/

#include "threadpool_unix.h"
#include "sql_table.h"
#include "field.h"
#include "sql_show.h"
#include "sql_class.h"
#include "my_sys.h"


namespace Show {

static ST_FIELD_INFO groups_fields_info[] =
{
  {"GROUP_ID", 6, MYSQL_TYPE_LONG, 0, 0, 0, SKIP_OPEN_TABLE},
  {"CONNECTIONS", 6, MYSQL_TYPE_LONG, 0, 0, 0, SKIP_OPEN_TABLE},
  {"THREADS", 6, MYSQL_TYPE_LONG, 0, 0, 0, SKIP_OPEN_TABLE},
  {"ACTIVE_THREADS", 6, MYSQL_TYPE_LONG, 0, 0, 0, SKIP_OPEN_TABLE},
  {"STANDBY_THREADS", 6, MYSQL_TYPE_LONG, 0, 0, 0, SKIP_OPEN_TABLE},
  {"QUEUE_LENGTH", 6, MYSQL_TYPE_LONG, 0, 0, 0, SKIP_OPEN_TABLE},
  {"HAS_LISTENER", 1, MYSQL_TYPE_TINY, 0, 0, 0, SKIP_OPEN_TABLE},
  {"IS_STALLED", 1, MYSQL_TYPE_TINY, 0, 0, 0, SKIP_OPEN_TABLE},
  {0, 0, MYSQL_TYPE_STRING, 0, 0, 0, SKIP_OPEN_TABLE}
};

} // namespace Show


static int groups_fill_table(THD* thd, TABLE_LIST* tables, Item*)
{
  if (!all_groups)
    return 0;

  TABLE* table = tables->table;
  for (uint i = 0; i < MAX_THREAD_GROUPS && all_groups[i].pollfd != -1; i++)
  {
    thread_group_t* group = &all_groups[i];

    mysql_mutex_lock(&group->mutex);

    /* ID */
    table->field[0]->store(i, true);
    /* CONNECTION_COUNT */
    table->field[1]->store(group->connection_count, true);
    /* THREAD_COUNT */
    table->field[2]->store(group->thread_count, true);
    /* ACTIVE_THREAD_COUNT */
    table->field[3]->store(group->active_thread_count, true);
    /* STANDBY_THREAD_COUNT */
    table->field[4]->store(group->waiting_thread_count, true);
    /* QUEUE LENGTH */
    uint queue_len = group->high_prio_queue.elements()
      + group->queue.elements();
    table->field[5]->store(queue_len, true);
    /* HAS_LISTENER */
    table->field[6]->store((longlong)(group->listener != 0), true);
    /* IS_STALLED */
    table->field[7]->store(group->stalled, true);

    mysql_mutex_unlock(&group->mutex);

    if (schema_table_store_record(thd, table))
      return 1;
  }
  return 0;
}


static int groups_init(void* p)
{
  ST_SCHEMA_TABLE* schema = (ST_SCHEMA_TABLE*)p;
  schema->fields_info = Show::groups_fields_info;
  schema->fill_table = groups_fill_table;
  return 0;
}


namespace Show {

static ST_FIELD_INFO queues_field_info[] =
{
  {"GROUP_ID", 6, MYSQL_TYPE_LONG, 0, 0, 0, SKIP_OPEN_TABLE},
  {"POSITION", 6, MYSQL_TYPE_LONG, 0, 0, 0, SKIP_OPEN_TABLE},
  {"PRIORITY", 1, MYSQL_TYPE_LONG, 0, 0, 0, SKIP_OPEN_TABLE},
  {"CONNECTION_ID", 19, MYSQL_TYPE_LONGLONG, 0, MY_I_S_UNSIGNED, 0, SKIP_OPEN_TABLE},
  {"QUEUEING_TIME_MICROSECONDS", 19, MYSQL_TYPE_LONGLONG, 0, 0, 0, SKIP_OPEN_TABLE},
  {0, 0, MYSQL_TYPE_STRING, 0, 0, 0, SKIP_OPEN_TABLE}
};

} // namespace Show

typedef connection_queue_t::Iterator connection_queue_iterator;

static int queues_fill_table(THD* thd, TABLE_LIST* tables, Item*)
{
  if (!all_groups)
    return 0;

  TABLE* table = tables->table;
  for (uint group_id = 0;
    group_id < MAX_THREAD_GROUPS && all_groups[group_id].pollfd != -1;
    group_id++)
  {
    thread_group_t* group = &all_groups[group_id];

    mysql_mutex_lock(&group->mutex);
    bool err = false;
    int pos = 0;
    ulonglong now = my_microsecond_getsystime();
    connection_queue_t queues[NQUEUES] = {group->high_prio_queue, group->queue};
    for (uint prio = 0; prio < NQUEUES && !err; prio++)
    {
      connection_queue_iterator it(queues[prio]);
      connection_t* c;
      while ((c = it++) != 0)
      {
        /* GROUP_ID */
        table->field[0]->store(group_id, true);
        /* POSITION */
        table->field[1]->store(pos++, true);
        /* PRIORITY */
        table->field[2]->store(prio, true);
        /* CONNECTION_ID */
        if (c->thd != nullptr) {
          table->field[3]->store(c->thd->thread_id(), true);
        } else {
          table->field[3]->store(0, true);
        }
        /* QUEUEING_TIME */
        table->field[4]->store(now - c->enqueue_time, true);

        err = schema_table_store_record(thd, table);
        if (err)
          break;
      }
    }
    mysql_mutex_unlock(&group->mutex);
    if (err)
      return 1;
  }
  return 0;
}

static int queues_init(void* p)
{
  ST_SCHEMA_TABLE* schema = (ST_SCHEMA_TABLE*)p;
  schema->fields_info = Show::queues_field_info;
  schema->fill_table = queues_fill_table;
  return 0;
}

namespace Show {

static ST_FIELD_INFO stats_fields_info[] =
{
  {"GROUP_ID", 6, MYSQL_TYPE_LONG, 0, 0, 0, SKIP_OPEN_TABLE},
  {"THREAD_CREATIONS", 19, MYSQL_TYPE_LONGLONG, 0, 0, 0, SKIP_OPEN_TABLE},
  {"THREAD_CREATIONS_DUE_TO_STALL", 19, MYSQL_TYPE_LONGLONG, 0, 0, 0, SKIP_OPEN_TABLE},
  {"WAKES", 19, MYSQL_TYPE_LONGLONG, 0, 0, 0, SKIP_OPEN_TABLE},
  {"WAKES_DUE_TO_STALL", 19, MYSQL_TYPE_LONGLONG, 0, 0, 0, SKIP_OPEN_TABLE},
  {"THROTTLES", 19, MYSQL_TYPE_LONGLONG, 0, 0, 0, SKIP_OPEN_TABLE},
  {"STALLS", 19, MYSQL_TYPE_LONGLONG, 0, 0, 0, SKIP_OPEN_TABLE},
  {"POLLS_BY_LISTENER", 19, MYSQL_TYPE_LONGLONG, 0, 0, 0, SKIP_OPEN_TABLE},
  {"POLLS_BY_WORKER", 19, MYSQL_TYPE_LONGLONG, 0, 0, 0, SKIP_OPEN_TABLE},
  {"DEQUEUES_BY_LISTENER", 19, MYSQL_TYPE_LONGLONG, 0, 0, 0, SKIP_OPEN_TABLE},
  {"DEQUEUES_BY_WORKER", 19, MYSQL_TYPE_LONGLONG, 0, 0, 0, SKIP_OPEN_TABLE},
  {0, 0, MYSQL_TYPE_STRING, 0, 0, 0, SKIP_OPEN_TABLE}
};

} // namespace Show


static int stats_fill_table(THD* thd, TABLE_LIST* tables, Item*)
{
  if (!all_groups)
    return 0;

  TABLE* table = tables->table;
  for (uint i = 0; i < MAX_THREAD_GROUPS && all_groups[i].pollfd != -1; i++)
  {
    table->field[0]->store(i, true);
    thread_group_t* group = &all_groups[i];

    mysql_mutex_lock(&group->mutex);
    thread_group_counters_t* counters = &group->counters;
    table->field[1]->store(counters->thread_creations, true);
    table->field[2]->store(counters->thread_creations_due_to_stall, true);
    table->field[3]->store(counters->wakes, true);
    table->field[4]->store(counters->wakes_due_to_stall, true);
    table->field[5]->store(counters->throttles, true);
    table->field[6]->store(counters->stalls, true);
    table->field[7]->store(counters->polls[LISTENER], true);
    table->field[8]->store(counters->polls[WORKER], true);
    table->field[9]->store(counters->dequeues[LISTENER], true);
    table->field[10]->store(counters->dequeues[WORKER], true);
    mysql_mutex_unlock(&group->mutex);
    if (schema_table_store_record(thd, table))
      return 1;
  }
  return 0;
}

static int stats_init(void* p)
{
  ST_SCHEMA_TABLE* schema = (ST_SCHEMA_TABLE*)p;
  schema->fields_info = Show::stats_fields_info;
  schema->fill_table = stats_fill_table;
  return 0;
}


namespace Show {

static ST_FIELD_INFO waits_fields_info[] =
{
  {"REASON", 16, MYSQL_TYPE_STRING, 0, 0, 0, SKIP_OPEN_TABLE},
  {"COUNT", 19, MYSQL_TYPE_LONGLONG, 0, 0, 0, SKIP_OPEN_TABLE},
  {0, 0, MYSQL_TYPE_STRING, 0, 0, 0, SKIP_OPEN_TABLE}
};

} // namespace Show

/* See thd_wait_type enum for explanation*/
static const LEX_CSTRING wait_reasons[THD_WAIT_LAST] =
{
  {STRING_WITH_LEN("UNKNOWN")},
  {STRING_WITH_LEN("SLEEP")},
  {STRING_WITH_LEN("DISKIO")},
  {STRING_WITH_LEN("ROW_LOCK")},
  {STRING_WITH_LEN("GLOBAL_LOCK")},
  {STRING_WITH_LEN("META_DATA_LOCK")},
  {STRING_WITH_LEN("TABLE_LOCK")},
  {STRING_WITH_LEN("USER_LOCK")},
  {STRING_WITH_LEN("BINLOG")},
  {STRING_WITH_LEN("GROUP_COMMIT")},
  {STRING_WITH_LEN("SYNC")},
  {STRING_WITH_LEN("NET")}
};

extern Atomic_int64 tp_waits[THD_WAIT_LAST];

static int waits_fill_table(THD* thd, TABLE_LIST* tables, Item*)
{
  if (!all_groups)
    return 0;

  TABLE* table = tables->table;
  for (unsigned int i = 0; i < THD_WAIT_LAST; i++)
  {
    table->field[0]->store(wait_reasons[i].str, wait_reasons[i].length, system_charset_info);
    table->field[1]->store(tp_waits[i].atomic_get(), true);
    if (schema_table_store_record(thd, table))
      return 1;
  }
  return 0;
}

static int waits_init(void* p)
{
  ST_SCHEMA_TABLE* schema = (ST_SCHEMA_TABLE*)p;
  schema->fields_info = Show::waits_fields_info;
  schema->fill_table = waits_fill_table;
  return 0;
}

static struct st_mysql_information_schema plugin_descriptor =
{ MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION };

mysql_declare_plugin(thread_pool_info)
{
  MYSQL_INFORMATION_SCHEMA_PLUGIN,
  &plugin_descriptor,
  "THREAD_POOL_GROUPS",
  "Vladislav Vaintroub",
  "Provides information about threadpool groups.",
  PLUGIN_LICENSE_GPL,
  groups_init,
  0,
  0x0100,
  NULL,
  NULL,
  NULL,
  0,
},
{
  MYSQL_INFORMATION_SCHEMA_PLUGIN,
  &plugin_descriptor,
  "THREAD_POOL_QUEUES",
  "Vladislav Vaintroub",
  "Provides information about threadpool queues.",
  PLUGIN_LICENSE_GPL,
  queues_init,
  0,
  0x0100,
  NULL,
  NULL,
  NULL,
  0,
},
{
  MYSQL_INFORMATION_SCHEMA_PLUGIN,
  &plugin_descriptor,
  "THREAD_POOL_STATS",
  "Vladislav Vaintroub",
  "Provides performance counter information for threadpool.",
  PLUGIN_LICENSE_GPL,
  stats_init,
  0,
  0x0100,
  NULL,
  NULL,
  NULL,
  0,
},
{
  MYSQL_INFORMATION_SCHEMA_PLUGIN,
  &plugin_descriptor,
  "THREAD_POOL_WAITS",
  "Vladislav Vaintroub",
  "Provides wait counters for threadpool.",
  PLUGIN_LICENSE_GPL,
  waits_init,
  0,
  0x0100,
  NULL,
  NULL,
  NULL,
  0,
}
mysql_declare_plugin_end;