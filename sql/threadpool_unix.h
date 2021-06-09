#ifndef THREADPOOL_UNIX_H_
#define THREADPOOL_UNIX_H_

#include <my_global.h>
#include <sql_plist.h>
// #include <my_pthread.h>
#include <mysqld.h>
#include <threadpool.h>
#include <violite.h>

#ifdef __linux__
#include <sys/epoll.h>
typedef struct epoll_event native_event;
#endif
#if defined (__FreeBSD__) || defined (__APPLE__)
#include <sys/event.h>
typedef struct kevent native_event;
#endif
#if defined (__sun)
#include <port.h>
typedef port_event_t native_event;
#endif

struct thread_group_t;

/* Per-thread structure for workers */
struct worker_thread_t
{
  ulonglong  event_count; /* number of request handled by this thread */
  thread_group_t* thread_group;   
  worker_thread_t *next_in_list;
  worker_thread_t **prev_in_list;
  
  mysql_cond_t  cond;
  bool          woken;
};

typedef I_P_List<worker_thread_t, I_P_List_adapter<worker_thread_t,
                 &worker_thread_t::next_in_list,
                 &worker_thread_t::prev_in_list> 
                 >
worker_list_t;

struct connection_t
{
  THD *thd;
  thread_group_t *thread_group;
  connection_t *next_in_queue;
  connection_t **prev_in_queue;
  ulonglong abs_wait_timeout;
  ulonglong enqueue_time;
  bool logged_in;
  bool bound_to_poll_descriptor;
  bool waiting;
  uint tickets;
};

typedef I_P_List<connection_t,
                     I_P_List_adapter<connection_t,
                                      &connection_t::next_in_queue,
                                      &connection_t::prev_in_queue>,
                     I_P_List_counter,
                     I_P_List_fast_push_back<connection_t> >
connection_queue_t;

const int NQUEUES = 2; /* We have high and low priority queues*/

enum operation_origin
{
  WORKER,
  LISTENER
};

struct thread_group_counters_t
{
  ulonglong thread_creations;
  ulonglong thread_creations_due_to_stall;
  ulonglong wakes;
  ulonglong wakes_due_to_stall;
  ulonglong throttles;
  ulonglong stalls;
  ulonglong dequeues[2];
  ulonglong polls[2];
};

struct thread_group_t 
{
  mysql_mutex_t mutex;
  connection_queue_t queue;
  connection_queue_t high_prio_queue;
  worker_list_t waiting_threads; 
  worker_thread_t *listener;
  pthread_attr_t *pthread_attr;
  int  pollfd;
  int  thread_count;
  int  dump_thread_count;
  int  active_thread_count;
  int  connection_count;
  int  waiting_thread_count;
  /* Stats for the deadlock detection timer routine.*/
  int io_event_count;
  int queue_event_count;
  ulonglong last_thread_creation_time;
  int  shutdown_pipe[2];
  bool shutdown;
  bool stalled;
  thread_group_counters_t counters;
} MY_ALIGNED(512);

#define TP_INCREMENT_GROUP_COUNTER(group,var) do {group->counters.var++;}while(0)

extern thread_group_t all_groups[MAX_THREAD_GROUPS];

#endif // THREADPOOL_UNIX_H_
