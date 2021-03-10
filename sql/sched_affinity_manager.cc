#include "sql/sched_affinity_manager.h"

#include "mysql/components/services/log_builtins.h"
#include "mysqld_error.h"
#include "sql/mysqld.h"

#ifdef HAVE_LIBNUMA
namespace sched_affinity {

const Thread_type thread_types[] = {
    Thread_type::FOREGROUND,         Thread_type::LOG_WRITER,
    Thread_type::LOG_FLUSHER,        Thread_type::LOG_WRITE_NOTIFIER,
    Thread_type::LOG_FLUSH_NOTIFIER, Thread_type::LOG_CLOSER,
    Thread_type::LOG_CHECKPOINTER,   Thread_type::PURGE_COORDINATOR};

class Lock_guard {
 public:
  explicit Lock_guard(mysql_mutex_t &mutex) {
    m_mutex = &mutex;
    mysql_mutex_lock(m_mutex);
  }
  Lock_guard(const Lock_guard &) = delete;
  Lock_guard &operator=(const Lock_guard &) = delete;
  ~Lock_guard() { mysql_mutex_unlock(m_mutex); }

 private:
  mysql_mutex_t *m_mutex;
};

Sched_affinity_manager_numa::Sched_affinity_manager_numa()
    : Sched_affinity_manager() {
  mysql_mutex_init(key_sched_affinity_mutex, &m_mutex, nullptr);

  m_total_cpu_num = 0;
  m_total_node_num = 0;
  m_cpu_num_per_node = 0;
  m_process_bitmask = nullptr;
  for (const auto &i : thread_types) {
    m_thread_sched_enabled[i] = false;
    m_thread_bitmask[i] = nullptr;
  }
}

Sched_affinity_manager_numa::~Sched_affinity_manager_numa() {
  mysql_mutex_destroy(&m_mutex);

  if (m_process_bitmask != nullptr) {
    numa_free_cpumask(m_process_bitmask);
    m_process_bitmask = nullptr;
  }
  for (const auto &i : thread_types) {
    if (m_thread_bitmask[i] != nullptr) {
      numa_free_cpumask(m_thread_bitmask[i]);
      m_thread_bitmask[i] = nullptr;
    }
  }
  for (auto sched_affinity_group : m_sched_affinity_group) {
    if (sched_affinity_group.avail_cpu_mask != nullptr) {
      numa_free_cpumask(sched_affinity_group.avail_cpu_mask);
      sched_affinity_group.avail_cpu_mask = nullptr;
    }
  }
}

bool Sched_affinity_manager_numa::init(
    const std::map<Thread_type, const char *> &sched_affinity_parameter) {
  if (!init_sched_affinity_info(sched_affinity_parameter)) {
    return false;
  }
  if (!init_sched_affinity_group()) {
    return false;
  }
  return true;
}

bool Sched_affinity_manager_numa::init_sched_affinity_info(
    const std::map<Thread_type, const char *> &sched_affinity_parameter) {
  m_total_cpu_num = numa_num_configured_cpus();
  m_total_node_num = numa_num_configured_nodes();
  m_cpu_num_per_node = m_total_cpu_num / m_total_node_num;

  m_process_bitmask = numa_allocate_cpumask();
  numa_sched_getaffinity(0, m_process_bitmask);

  for (const auto &p : sched_affinity_parameter) {
    if (p.second == nullptr) {
      continue;
    } else if ((m_thread_bitmask[p.first] = numa_parse_cpustring(p.second)) ==
               nullptr) {
      LogErr(ERROR_LEVEL, ER_CANT_PARSE_CPU_STRING, p.second);
      return false;
    } else if (!check_thread_process_compatibility(m_thread_bitmask[p.first],
                                                   m_process_bitmask)) {
      LogErr(ERROR_LEVEL, ER_SCHED_AFFINITY_THREAD_PROCESS_CONFLICT);
      return false;
    }
    m_thread_sched_enabled[p.first] = true;
  }

  if (m_thread_sched_enabled[Thread_type::FOREGROUND]) {
    for (const auto &i : thread_types) {
      if (i != Thread_type::FOREGROUND && m_thread_sched_enabled[i] &&
          !check_foreground_background_compatibility(
              m_thread_bitmask[Thread_type::FOREGROUND], m_thread_bitmask[i])) {
        LogErr(WARNING_LEVEL, ER_SCHED_AFFINITY_FOREGROUND_BACKGROUND_CONFLICT);
      }
    }
  }

  return true;
}

bool Sched_affinity_manager_numa::init_sched_affinity_group() {
  if (!m_thread_sched_enabled[Thread_type::FOREGROUND]) {
    return true;
  }
  m_sched_affinity_group.resize(m_total_node_num);

  bool group_available = false;
  for (int i = 0; i < m_total_node_num; ++i) {
    m_sched_affinity_group[i].avail_cpu_num = 0;
    m_sched_affinity_group[i].avail_cpu_mask = numa_allocate_cpumask();
    m_sched_affinity_group[i].assigned_thread_num = 0;

    for (int j = m_cpu_num_per_node * i; j < m_cpu_num_per_node * (i + 1);
         ++j) {
      if (numa_bitmask_isbitset(m_thread_bitmask[Thread_type::FOREGROUND], j)) {
        numa_bitmask_setbit(m_sched_affinity_group[i].avail_cpu_mask, j);
        ++m_sched_affinity_group[i].avail_cpu_num;
        group_available = true;
      }
    }
  }

  return group_available;
}

bool Sched_affinity_manager_numa::check_foreground_background_compatibility(
    bitmask *bm_foreground, bitmask *bm_background) {
  if (bm_foreground == nullptr || bm_background == nullptr) {
    return true;
  }
  for (auto i = 0; i < m_total_cpu_num; ++i) {
    if (numa_bitmask_isbitset(bm_foreground, i) &&
        numa_bitmask_isbitset(bm_background, i)) {
      return false;
    }
  }
  return true;
}

bool Sched_affinity_manager_numa::check_thread_process_compatibility(
    bitmask *bm_thread, bitmask *bm_proc) {
  if (bm_thread == nullptr || bm_proc == nullptr) {
    return true;
  }
  for (auto i = 0; i < m_total_cpu_num; ++i) {
    if (numa_bitmask_isbitset(bm_thread, i) &&
        !numa_bitmask_isbitset(bm_proc, i)) {
      return false;
    }
  }
  return true;
}

bool Sched_affinity_manager_numa::bind_to_group(int &out) {
  if (!m_thread_sched_enabled[Thread_type::FOREGROUND]) {
    out = -1;
    return true;
  }

  const Lock_guard lock(m_mutex);

  auto best_index = -1;
  for (auto i = 0u; i < m_sched_affinity_group.size(); ++i) {
    if (m_sched_affinity_group[i].avail_cpu_num == 0) {
      continue;
    }
    if (best_index == -1 ||
        m_sched_affinity_group[i].assigned_thread_num *
                m_sched_affinity_group[best_index].avail_cpu_num <
            m_sched_affinity_group[best_index].assigned_thread_num *
                m_sched_affinity_group[i].avail_cpu_num) {
      best_index = i;
    }
  }

  if (best_index == -1) {
    out = -1;
    return false;
  }
  auto ret = numa_sched_setaffinity(
      0, m_sched_affinity_group[best_index].avail_cpu_mask);
  if (ret == 0) {
    ++m_sched_affinity_group[best_index].assigned_thread_num;
    out = best_index;
    return true;
  } else {
    out = -1;
    return false;
  }
}

bool Sched_affinity_manager_numa::unbind_from_group(const int &index) {
  if (!m_thread_sched_enabled[Thread_type::FOREGROUND]) {
    return true;
  }
  if (index < 0 || index >= static_cast<int>(m_sched_affinity_group.size())) {
    return false;
  }
  const Lock_guard lock(m_mutex);
  if (m_sched_affinity_group[index].assigned_thread_num > 0) {
    --m_sched_affinity_group[index].assigned_thread_num;
    return true;
  } else {
    return false;
  }
}

bool Sched_affinity_manager_numa::bind_to_target(const Thread_type &thread_type) {
  if (thread_type == Thread_type::FOREGROUND) {
    return false;
  }
  if (!m_thread_sched_enabled[thread_type]) {
    return true;
  }
  auto ret = numa_sched_setaffinity(0, m_thread_bitmask[thread_type]);
  return ret == 0 ? true : false;
}

void Sched_affinity_manager_numa::take_snapshot(char *buff, int buff_size) {
  if (buff == nullptr || buff_size <= 0) {
    return;
  }
  const Lock_guard lock(m_mutex);
  int used_buff_size = 0;
  for (auto sched_affinity_group : m_sched_affinity_group) {
    int used = snprintf(buff + used_buff_size, buff_size - used_buff_size,
                        "%d/%d; ", sched_affinity_group.assigned_thread_num,
                        sched_affinity_group.avail_cpu_num);
    if (used > 0) {
      used_buff_size += used;
    }
    if (used_buff_size + 1 >= buff_size) {
      break;
    }
  }
}

int Sched_affinity_manager_numa::get_total_node_number() {
  return m_total_node_num;
}
int Sched_affinity_manager_numa::get_cpu_number_per_node() {
  return m_cpu_num_per_node;
}
}  // namespace sched_affinity
#endif /* HAVE_LIBNUMA */

namespace sched_affinity {
void Sched_affinity_manager_dummy::take_snapshot(char *buff, int buff_size) {
  if (buff == nullptr || buff_size <= 0) {
    return;
  }
  buff[0] = '\0';
}

static Sched_affinity_manager *sched_affinity_manager = nullptr;

Sched_affinity_manager *Sched_affinity_manager::create_instance(
    const std::map<Thread_type, const char *> &sched_affinity_parameter) {
  Sched_affinity_manager::free_instance();
#ifdef HAVE_LIBNUMA
  if (numa_available() == -1) {
    LogErr(WARNING_LEVEL, ER_NUMA_AVAILABLE_TEST_FAIL);
    LogErr(INFORMATION_LEVEL, ER_USE_DUMMY_SCHED_AFFINITY_MANAGER);
    sched_affinity_manager = new Sched_affinity_manager_dummy();
  } else {
    sched_affinity_manager = new Sched_affinity_manager_numa();
  }
#else
  LogErr(WARNING_LEVEL, ER_LIBNUMA_TEST_FAIL);
  LogErr(INFORMATION_LEVEL, ER_USE_DUMMY_SCHED_AFFINITY_MANAGER);
  sched_affinity_manager = new Sched_affinity_manager_dummy();
#endif /* HAVE_LIBNUMA */
  if (!sched_affinity_manager->init(sched_affinity_parameter)) {
    return nullptr;
  }
  return sched_affinity_manager;
}

Sched_affinity_manager *Sched_affinity_manager::get_instance() {
  return sched_affinity_manager;
}

void Sched_affinity_manager::free_instance() {
  if (sched_affinity_manager != nullptr) {
    delete sched_affinity_manager;
    sched_affinity_manager = nullptr;
  }
}
}  // namespace sched_affinity
