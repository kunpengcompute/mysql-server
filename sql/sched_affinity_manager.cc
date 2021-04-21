/*****************************************************************************
Copyright (c) 2020, Huawei Technologies Co., Ltd. All Rights Reserved.
This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.
This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.
*****************************************************************************/

#include "sql/sched_affinity_manager.h"

#include <sys/syscall.h>

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
}

Sched_affinity_manager_numa::~Sched_affinity_manager_numa() {
  mysql_mutex_destroy(&m_mutex);

  for (const auto &i : thread_types) {
    if (m_thread_bitmask[i] != nullptr) {
      numa_free_cpumask(m_thread_bitmask[i]);
      m_thread_bitmask[i] = nullptr;
    }
  }
  for (auto sched_affinity_group : m_sched_affinity_group) {
    for (auto group : sched_affinity_group.second) {
      if (group.avail_cpu_mask != nullptr) {
        numa_free_cpumask(group.avail_cpu_mask);
        group.avail_cpu_mask = nullptr;
      }
    }
  }
}

bool Sched_affinity_manager_numa::init(
    const std::map<Thread_type, const char *> &sched_affinity_parameter,
    bool numa_aware) {
  m_total_cpu_num = numa_num_configured_cpus();
  m_total_node_num = numa_num_configured_nodes();
  m_cpu_num_per_node = m_total_cpu_num / m_total_node_num;

  m_thread_bitmask.clear();
  m_sched_affinity_group.clear();
  m_thread_pid.clear();
  for (const auto &thread_type : thread_types) {
    if (sched_affinity_parameter.find(thread_type) ==
        sched_affinity_parameter.end()) {
      continue;
    }
    m_thread_pid[thread_type] = std::set<pid_t>();
    auto cpu_string = sched_affinity_parameter.find(thread_type)->second;
    if (cpu_string != nullptr &&
        !init_sched_affinity_info(std::string(cpu_string),
                                  m_thread_bitmask[thread_type])) {
      return false;
    }
    if (is_thread_sched_enabled(thread_type) &&
        !init_sched_affinity_group(m_thread_bitmask[thread_type], numa_aware,
                                   m_sched_affinity_group[thread_type])) {
      return false;
    }
  }

  return true;
}

bool Sched_affinity_manager_numa::init_sched_affinity_info(
    const std::string &cpu_string, bitmask *&group_bitmask) {
  group_bitmask = nullptr;
  if (cpu_string.empty()) {
    return true;
  }
  std::pair<std::string, bool> normalized_result =
      normalize_cpu_string(cpu_string);
  if (normalized_result.second == false) {
    LogErr(ERROR_LEVEL, ER_CANT_PARSE_CPU_STRING, cpu_string.c_str());
    return false;
  }
  group_bitmask = numa_parse_cpustring(normalized_result.first.c_str());
  if (group_bitmask == nullptr) {
    LogErr(ERROR_LEVEL, ER_CANT_PARSE_CPU_STRING, cpu_string.c_str());
    return false;
  }
  return true;
}

bool Sched_affinity_manager_numa::init_sched_affinity_group(
    const bitmask *group_bitmask, const bool numa_aware,
    std::vector<Sched_affinity_group> &sched_affinity_group) {
  if (numa_aware) {
    sched_affinity_group.resize(m_total_node_num);
    for (auto node_id = 0; node_id < m_total_node_num; ++node_id) {
      sched_affinity_group[node_id].avail_cpu_num = 0;
      sched_affinity_group[node_id].avail_cpu_mask = numa_allocate_cpumask();
      sched_affinity_group[node_id].assigned_thread_num = 0;
      for (auto cpu_id = m_cpu_num_per_node * node_id;
           cpu_id < m_cpu_num_per_node * (node_id + 1); ++cpu_id) {
        if (numa_bitmask_isbitset(group_bitmask, cpu_id)) {
          numa_bitmask_setbit(sched_affinity_group[node_id].avail_cpu_mask,
                              cpu_id);
          ++sched_affinity_group[node_id].avail_cpu_num;
        }
      }
    }
  } else {
    sched_affinity_group.resize(1);
    sched_affinity_group[0].avail_cpu_num = 0;
    sched_affinity_group[0].avail_cpu_mask = numa_allocate_cpumask();
    copy_bitmask_to_bitmask(const_cast<bitmask *>(group_bitmask),
                            sched_affinity_group[0].avail_cpu_mask);
    sched_affinity_group[0].assigned_thread_num = 0;
    for (auto cpu_id = 0; cpu_id < m_total_cpu_num; ++cpu_id) {
      if (numa_bitmask_isbitset(group_bitmask, cpu_id)) {
        ++sched_affinity_group[0].avail_cpu_num;
      }
    }
  }
  return true;
}

bool Sched_affinity_manager_numa::rebalance_group(
    const Thread_type thread_type) {
  const Lock_guard lock(m_mutex);
  // TODO
  return true;
}

bool Sched_affinity_manager_numa::are_bitmasks_overlapped(bitmask *first,
                                                          bitmask *second) {
  if (first == nullptr || second == nullptr) {
    return true;
  }
  for (auto i = 0; i < m_total_cpu_num; ++i) {
    if (numa_bitmask_isbitset(first, i) && numa_bitmask_isbitset(second, i)) {
      return false;
    }
  }
  return true;
}

bool Sched_affinity_manager_numa::is_thread_sched_enabled(
    const Thread_type thread_type) {
  auto it = m_thread_bitmask.find(thread_type);
  return (it != m_thread_bitmask.end() && it->second != nullptr) ? true : false;
}

bool Sched_affinity_manager_numa::register_thread(const Thread_type thread_type,
                                                  const pid_t pid) {
  const Lock_guard lock(m_mutex);
  m_thread_pid[thread_type].insert(pid);
  if (!is_thread_sched_enabled(thread_type)) {
    return true;
  }

  bind_to_group(pid);
  return true;
}

bool Sched_affinity_manager_numa::unregister_thread(
    const Thread_type thread_type, const pid_t pid) {
  const Lock_guard lock(m_mutex);
  m_thread_pid[thread_type].erase(pid);
  if (!is_thread_sched_enabled(thread_type)) {
    return true;
  }

  unbind_from_group(pid);
  return true;
}

Thread_type Sched_affinity_manager_numa::get_thread_type_by_pid(
    const pid_t pid) {
  for (const auto &thread_pid : m_thread_pid) {
    if (thread_pid.second.find(pid) != thread_pid.second.end()) {
      return thread_pid.first;
    }
  }
  return Thread_type::UNDEFINED;
}

bool Sched_affinity_manager_numa::bind_to_group(const pid_t pid) {
  auto thread_type = get_thread_type_by_pid(pid);
  if (thread_type == Thread_type::UNDEFINED ||
      !is_thread_sched_enabled(thread_type)) {
    return false;
  }
  auto &sched_affinity_group = m_sched_affinity_group[thread_type];

  const int INVALID_INDEX = -1;
  auto best_index = INVALID_INDEX;
  for (auto i = 0u; i < sched_affinity_group.size(); ++i) {
    if (sched_affinity_group[i].avail_cpu_num == 0) {
      continue;
    }
    if (best_index == INVALID_INDEX ||
        sched_affinity_group[i].assigned_thread_num *
                sched_affinity_group[best_index].avail_cpu_num <
            sched_affinity_group[best_index].assigned_thread_num *
                sched_affinity_group[i].avail_cpu_num) {
      best_index = i;
    }
  }

  if (best_index == INVALID_INDEX) {
    return false;
  }
  auto ret = numa_sched_setaffinity(
      pid, sched_affinity_group[best_index].avail_cpu_mask);
  if (ret == 0) {
    ++sched_affinity_group[best_index].assigned_thread_num;
    m_pid_group_id[pid] = best_index;
    return true;
  }
  return false;
}

bool Sched_affinity_manager_numa::unbind_from_group(const pid_t pid) {
  auto thread_type = get_thread_type_by_pid(pid);
  if (thread_type == Thread_type::UNDEFINED) {
    return false;
  }
  if (!is_thread_sched_enabled(thread_type)) {
    return false;
  }
  auto &sched_affinity_group = m_sched_affinity_group[thread_type];
  auto index = m_pid_group_id.find(pid);
  if (index == m_pid_group_id.end() ||
      index->second >= static_cast<int>(sched_affinity_group.size())) {
    return false;
  }
  m_pid_group_id.erase(index);
  return true;
}

void Sched_affinity_manager_numa::take_snapshot(char *buff, int buff_size) {
  // TODO support more status
  if (buff == nullptr || buff_size <= 0) {
    return;
  }
  buff[0] = '\0';
  if (m_sched_affinity_group.find(Thread_type::FOREGROUND) ==
      m_sched_affinity_group.end()) {
    return;
  }
  const Lock_guard lock(m_mutex);
  int used_buff_size = 0;
  for (auto sched_affinity_group :
       m_sched_affinity_group[Thread_type::FOREGROUND]) {
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

bool Sched_affinity_manager_numa::check_cpu_string(
    const std::string &cpu_string) {
  return normalize_cpu_string(cpu_string).second;
}

std::pair<std::string, bool> Sched_affinity_manager_numa::normalize_cpu_string(
    const std::string &cpu_string) {
  std::string normalized_cpu_string = "";
  bool invalid_cpu_string = false;
  const int INVALID_CORE_ID = -1;
  int core_id = INVALID_CORE_ID;
  for (auto c : cpu_string) {
    switch (c) {
      case ' ':
        break;
      case '-':
      case ',':
        if (core_id == INVALID_CORE_ID) {
          invalid_cpu_string = true;
        } else {
          normalized_cpu_string += std::to_string(core_id);
          normalized_cpu_string += c;
          core_id = INVALID_CORE_ID;
        }
        break;
      case '0' ... '9':
        if (core_id == INVALID_CORE_ID) {
          core_id = (c - '0');
        } else {
          core_id = core_id * 10 + (c - '0');
        }
        break;
      default:
        invalid_cpu_string = true;
        break;
    }
    if (invalid_cpu_string) {
      break;
    }
  }
  if (core_id != INVALID_CORE_ID) {
    normalized_cpu_string += std::to_string(core_id);
  }
  if (!normalized_cpu_string.empty() &&
      (*normalized_cpu_string.rbegin() == '-' ||
       *normalized_cpu_string.rbegin() == ',')) {
    invalid_cpu_string = true;
  }
  if (invalid_cpu_string) {
    return std::make_pair(std::string(), false);
  }
  return std::make_pair(normalized_cpu_string, true);
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
    const std::map<Thread_type, const char *> &sched_affinity_parameter,
    bool numa_aware) {
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
  if (!sched_affinity_manager->init(sched_affinity_parameter, numa_aware)) {
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

pid_t gettid()
{
  return static_cast<pid_t>(syscall(SYS_gettid));
}

}  // namespace sched_affinity
