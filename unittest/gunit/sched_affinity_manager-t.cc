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

#include "my_config.h"
#include "sql/sched_affinity_manager.h"
#ifdef HAVE_LIBNUMA
#include <numa.h>
#endif
#include <string>
#include <iostream>
#include <thread>
#include <limits>
#include <algorithm>
#include <numeric>
#include <atomic>
#include <mutex>
#include <sys/syscall.h>

#include "gtest/gtest.h"

#ifdef HAVE_LIBNUMA

using ::sched_affinity::Sched_affinity_manager;
using ::sched_affinity::Sched_affinity_manager_numa;
using ::sched_affinity::Thread_type;
using ::testing::TestInfo;
using namespace std;

namespace sched_affinity {

const Thread_type thread_types[] = {
    Thread_type::FOREGROUND,         Thread_type::LOG_WRITER,
    Thread_type::LOG_FLUSHER,        Thread_type::LOG_WRITE_NOTIFIER,
    Thread_type::LOG_FLUSH_NOTIFIER, Thread_type::LOG_CLOSER,
    Thread_type::LOG_CHECKPOINTER,   Thread_type::PURGE_COORDINATOR};

class SchedAffinityManagerTest : public ::testing::Test {
 public:
  static pid_t gettid()
  {
      return static_cast<pid_t>(syscall(SYS_gettid));
  }

  void start_threads(int thread_number, std::vector<std::thread> &threads,
                     std::vector<pid_t> &thread_pids,
                     std::atomic<bool> &is_stopped) {
    using namespace std::chrono_literals;
    std::mutex mutex;
    is_stopped.store(false);
    threads.clear();
    for (auto i = 0; i < thread_number; ++i) {
      threads.push_back(std::thread([&mutex, &thread_pids, &is_stopped]() {
        auto pid = gettid();
        {
          std::lock_guard<std::mutex> gurad(mutex);
          thread_pids.push_back(pid);
        }
        while (!is_stopped.load()) {
          std::this_thread::sleep_for(100ms);
        }
      }));
    }

    while (true) {
      {
        std::lock_guard<std::mutex> gurad(mutex);
        if (static_cast<int>(thread_pids.size()) == thread_number) {
          break;
        }
      }
      std::this_thread::sleep_for(100ms);
    }
  }

 protected:
  bool skip_if_numa_unavailable() {
    if (numa_available() == -1) {
      SUCCEED() << "Skip test case as numa is unavailable.";
      return true;
    } else {
      return false;
    }
  }

  std::string convert_bitmask_to_string(Bitmask_shared_ptr& bitmask, int& min, int& max) {
    std::string res;

    int i = 0, total_cpu_num = numa_num_configured_cpus();
    bool flag1 = false, flag2 = false;
    for (; i < total_cpu_num; ) {
      int start = i, curr = i;

      while (curr < total_cpu_num 
             && numa_bitmask_isbitset(bitmask.get(), curr)) {
        max = curr;
        curr++;
        flag2 = true;
      }

      if(flag1 && flag2) {
        res += ",";
      }

      if(curr != start && !flag1) {
        flag1 = true;
        min = start;
      }

      if (curr != start && curr - start == 1) {
        res += std::to_string(start);
      } else if (curr != start) {
        res += std::to_string(start) + "-" + std::to_string(curr-1);
      }
      if (curr == start) {
        i++;
      } else {
        i += (curr - start);
      }

      flag2 = false;
    }

    return res;
  }

  bool check_bind_to_target(int index) {
    Bitmask_shared_ptr sched_bitmask = get_bitmask_shared_ptr(numa_allocate_cpumask());
    int ret = numa_sched_getaffinity(0, sched_bitmask.get());

    if (ret != -1) {
      bool flag = false;
      for (int i = 0; i < numa_num_configured_cpus(); i++) {
        if ((i == index && !numa_bitmask_isbitset(sched_bitmask.get(), i)) 
            || (i != index && numa_bitmask_isbitset(sched_bitmask.get(), i))) {
          flag = true;
          break;
        }
      }
      if (flag) {
        return false;
      }
    } else {
      return false;
    }

    return true;
  }

  int get_avail_node_num(int node_num) {
    int res = 0;
    Bitmask_shared_ptr bm = get_bitmask_shared_ptr(numa_get_run_node_mask());

    for (int i = 0; i < node_num; i++) {
      if (numa_bitmask_isbitset(bm.get(), i)) {
        res++;
      }
    }

    return res;
  }

  void init_avail_cpu_num_per_node(std::vector<int> &avail_cpu_num_per_node, int cpu_num_per_node, 
                                   Bitmask_shared_ptr& default_bitmask) {
    for (int i = 0; i < test_process_node_num; i++) {
      int count = 0;
      for (int j = i * cpu_num_per_node; j < (i + 1) * cpu_num_per_node; j++) {
        if (numa_bitmask_isbitset(default_bitmask.get(), j)) {
          count++;
        }
      }
      avail_cpu_num_per_node[i] = count;
    }
  }

  void init_avail_nodes_arr(std::vector<char> &avail_nodes_arr, int test_process_node_num) {
    Bitmask_shared_ptr test_process_node_mask = get_bitmask_shared_ptr(numa_get_run_node_mask());

    for (int i = 0, arr_index = 0; i < test_process_node_num; i++) {
      if (numa_bitmask_isbitset(test_process_node_mask.get(), i)) {
        avail_nodes_arr[arr_index++] = i;
      }
    }
  }

 protected:
  void SetUp() {
    if (skip_if_numa_unavailable()) {
      return;
    } else {
      test_process_cpu_num = numa_num_configured_cpus();
      test_process_node_num = numa_num_configured_nodes();

      ASSERT_GT(test_process_cpu_num, 0);
      ASSERT_GT(test_process_node_num, 0);

      cpu_num_per_node = test_process_cpu_num / test_process_node_num;
      test_avail_node_num = get_avail_node_num(test_process_node_num);

      
      Bitmask_shared_ptr default_bitmask = get_bitmask_shared_ptr(numa_allocate_cpumask());
      int ret = numa_sched_getaffinity(0, default_bitmask.get());
      ASSERT_NE(ret, -1);

      cpu_range_str = convert_bitmask_to_string(default_bitmask, cpu_range_min, cpu_range_max);

      avail_nodes_arr.resize(test_avail_node_num);
      avail_cpu_num_per_node.resize(test_process_node_num);

      init_avail_nodes_arr(avail_nodes_arr, test_process_node_num);
      init_avail_cpu_num_per_node(avail_cpu_num_per_node, cpu_num_per_node, default_bitmask);

      default_config = {
        {sched_affinity::Thread_type::FOREGROUND, nullptr},
        {sched_affinity::Thread_type::LOG_WRITER, nullptr},
        {sched_affinity::Thread_type::LOG_FLUSHER, nullptr},
        {sched_affinity::Thread_type::LOG_WRITE_NOTIFIER, nullptr},
        {sched_affinity::Thread_type::LOG_FLUSH_NOTIFIER, nullptr},
        {sched_affinity::Thread_type::LOG_CLOSER, nullptr},
        {sched_affinity::Thread_type::LOG_CHECKPOINTER, nullptr},
        {sched_affinity::Thread_type::PURGE_COORDINATOR, nullptr}};
    }
  }

  void TearDown() {

  }

 protected:
  int test_process_cpu_num;

  int test_process_node_num;

  int cpu_num_per_node;

  int test_avail_node_num;

  int cpu_range_min;

  int cpu_range_max;

  std::vector<char> avail_nodes_arr;

  std::vector<int> avail_cpu_num_per_node;

  std::string cpu_range_str;

  const int BUFFER_SIZE_1024 = 1024;

  std::map<Thread_type, const char *> default_config;

  Bitmask_shared_ptr default_bitmask;
};

TEST_F(SchedAffinityManagerTest, AllNullptrConfig) {
  if (skip_if_numa_unavailable()) {
    return;
  }

  std::map<sched_affinity::Thread_type, const char *> all_nullptr_config = {
      {sched_affinity::Thread_type::FOREGROUND, nullptr},
      {sched_affinity::Thread_type::LOG_WRITER, nullptr},
      {sched_affinity::Thread_type::LOG_FLUSHER, nullptr},
      {sched_affinity::Thread_type::LOG_WRITE_NOTIFIER, nullptr},
      {sched_affinity::Thread_type::LOG_FLUSH_NOTIFIER, nullptr},
      {sched_affinity::Thread_type::LOG_CLOSER, nullptr},
      {sched_affinity::Thread_type::LOG_CHECKPOINTER, nullptr},
      {sched_affinity::Thread_type::PURGE_COORDINATOR, nullptr}};

  auto instance =
      Sched_affinity_manager::create_instance(all_nullptr_config, false);
  ASSERT_NE(instance, nullptr);
  ASSERT_EQ(typeid(*instance), typeid(Sched_affinity_manager_numa));
  for (auto thread_type : thread_types) {
    ASSERT_FALSE(dynamic_cast<Sched_affinity_manager_numa*>(instance)->is_thread_sched_enabled(thread_type));
  }

  Sched_affinity_manager::free_instance();
}

TEST_F(SchedAffinityManagerTest, EmptyStringConfig) {
  if (skip_if_numa_unavailable()) {
    return;
  }

  std::map<sched_affinity::Thread_type, const char *> empty_string_config = {
      {sched_affinity::Thread_type::FOREGROUND, ""},
      {sched_affinity::Thread_type::LOG_WRITER, ""},
      {sched_affinity::Thread_type::LOG_FLUSHER, ""},
      {sched_affinity::Thread_type::LOG_WRITE_NOTIFIER, ""},
      {sched_affinity::Thread_type::LOG_FLUSH_NOTIFIER, ""},
      {sched_affinity::Thread_type::LOG_CLOSER, ""},
      {sched_affinity::Thread_type::LOG_CHECKPOINTER, ""},
      {sched_affinity::Thread_type::PURGE_COORDINATOR, ""}};

  auto instance =
      Sched_affinity_manager::create_instance(empty_string_config, false);
  ASSERT_NE(instance, nullptr);
  ASSERT_EQ(typeid(*instance), typeid(Sched_affinity_manager_numa));
  for (auto thread_type : thread_types) {
    ASSERT_FALSE(dynamic_cast<Sched_affinity_manager_numa *>(instance)
                     ->is_thread_sched_enabled(thread_type));
  }

  Sched_affinity_manager::free_instance();
}

TEST_F(SchedAffinityManagerTest, EmptyContainerConfig) {
  if (skip_if_numa_unavailable()) {
    return;
  }

  std::map<sched_affinity::Thread_type, const char *> empty_container_config =
      {};

  auto instance =
      Sched_affinity_manager::create_instance(empty_container_config, false);
  ASSERT_NE(instance, nullptr);
  ASSERT_EQ(typeid(*instance), typeid(Sched_affinity_manager_numa));
  for (auto thread_type : thread_types) {
    ASSERT_FALSE(dynamic_cast<Sched_affinity_manager_numa *>(instance)
                  ->is_thread_sched_enabled(thread_type));
  }

  Sched_affinity_manager::free_instance();
}

TEST_F(SchedAffinityManagerTest, NormalizeCpuString) {
  if (skip_if_numa_unavailable()) {
    return;
  }

  std::pair<std::string, bool> result = Sched_affinity_manager_numa::normalize_cpu_string("");
  ASSERT_TRUE(result.second);
  ASSERT_EQ("", result.first);

  result = Sched_affinity_manager_numa::normalize_cpu_string("  ");
  ASSERT_TRUE(result.second);
  ASSERT_EQ("", result.first);

  result = Sched_affinity_manager_numa::normalize_cpu_string("1-5,6,7");
  ASSERT_TRUE(result.second);
  ASSERT_EQ("1-5,6,7", result.first);

  result = Sched_affinity_manager_numa::normalize_cpu_string("01-2");
  ASSERT_TRUE(result.second);
  ASSERT_EQ("1-2", result.first);

  result = Sched_affinity_manager_numa::normalize_cpu_string("1-02");
  ASSERT_TRUE(result.second);
  ASSERT_EQ("1-2", result.first);

  result = Sched_affinity_manager_numa::normalize_cpu_string(" 1 - 5 ,6,7 ");
  ASSERT_TRUE(result.second);
  ASSERT_EQ("1-5,6,7", result.first);

  result = Sched_affinity_manager_numa::normalize_cpu_string("1-5,?6,7");
  ASSERT_FALSE(result.second);
}

TEST_F(SchedAffinityManagerTest, InitSchedAffinityInfo) {
  if (skip_if_numa_unavailable()) {
    return;
  }

  auto instance =
      Sched_affinity_manager::create_instance(default_config, false);
  auto true_type_instance =
      dynamic_cast<Sched_affinity_manager_numa *>(instance);
  Bitmask_shared_ptr bitmask = get_bitmask_shared_ptr(nullptr);

  bool result = true_type_instance->init_sched_affinity_info("", bitmask);
  ASSERT_TRUE(result);
  ASSERT_EQ(bitmask.get(), nullptr);
  bitmask.reset();

  result = true_type_instance->init_sched_affinity_info("1-3", bitmask);
  ASSERT_TRUE(result);
  ASSERT_NE(bitmask.get(), nullptr);
  Bitmask_shared_ptr expected = get_bitmask_shared_ptr(numa_parse_cpustring("1-3"));
  ASSERT_EQ(1, numa_bitmask_equal(expected.get(), bitmask.get()));
  bitmask.reset();

  result = true_type_instance->init_sched_affinity_info("??", bitmask);
  ASSERT_FALSE(result);
  ASSERT_EQ(bitmask.get(), nullptr);
  bitmask.reset();

  Sched_affinity_manager::free_instance();
}

TEST_F(SchedAffinityManagerTest, InitSchedAffinityGroup) {
  if (skip_if_numa_unavailable()) {
    return;
  }

  auto instance =
      Sched_affinity_manager::create_instance(default_config, false);
  auto true_type_instance =
      dynamic_cast<Sched_affinity_manager_numa *>(instance);

  Bitmask_shared_ptr bitmask = get_bitmask_shared_ptr(nullptr);
  std::vector<Sched_affinity_group> sched_affinity_groups;

  true_type_instance->init_sched_affinity_info("1-3", bitmask);

  bool result = true_type_instance->init_sched_affinity_group(bitmask, false, sched_affinity_groups);
  ASSERT_TRUE(result);
  ASSERT_EQ(1, sched_affinity_groups.size());
  ASSERT_EQ(
      1, numa_bitmask_equal(bitmask.get(), sched_affinity_groups[0].avail_cpu_mask.get()));
  ASSERT_EQ(0, sched_affinity_groups[0].assigned_thread_num);
  ASSERT_EQ(3, sched_affinity_groups[0].avail_cpu_num);

  sched_affinity_groups.clear();
  result = true_type_instance->init_sched_affinity_group(bitmask, true, sched_affinity_groups);
  ASSERT_TRUE(result);
  ASSERT_EQ(4, sched_affinity_groups.size());
  ASSERT_TRUE(
      std::all_of(sched_affinity_groups.begin(), sched_affinity_groups.end(),
                  [](Sched_affinity_group &sched_affinity_group) {
                    return sched_affinity_group.assigned_thread_num == 0;
                  }));
  ASSERT_EQ(3, std::accumulate(
                   sched_affinity_groups.begin(), sched_affinity_groups.end(),
                   0, [](int v, Sched_affinity_group &sched_affinity_group) {
                     return v + sched_affinity_group.avail_cpu_num;
                   }));

  Sched_affinity_manager::free_instance();
}

TEST_F(SchedAffinityManagerTest, IsThreadSchedEnabled) {
  using namespace std::chrono_literals;

  if (skip_if_numa_unavailable()) {
    return;
  }

  std::map<sched_affinity::Thread_type, const char *> config = {
      {sched_affinity::Thread_type::FOREGROUND, "1-3"}};

  auto instance = Sched_affinity_manager::create_instance(config, false);
  auto true_type_instance =
      dynamic_cast<Sched_affinity_manager_numa *>(instance);

  for (auto thread_type : thread_types) {
    if (thread_type == Thread_type::FOREGROUND) {
      ASSERT_TRUE(true_type_instance->is_thread_sched_enabled(thread_type));
    } else {
      ASSERT_FALSE(true_type_instance->is_thread_sched_enabled(thread_type));
    }
  }

  Sched_affinity_manager::free_instance();
}

TEST_F(SchedAffinityManagerTest, BindToGroup) {
  using namespace std::chrono_literals;

  if (skip_if_numa_unavailable()) {
    return;
  }

  std::map<sched_affinity::Thread_type, const char *> config = {
      {sched_affinity::Thread_type::FOREGROUND, "1-3"}};

  auto instance = Sched_affinity_manager::create_instance(config, false);
  auto true_type_instance =
      dynamic_cast<Sched_affinity_manager_numa *>(instance);

  std::map<Thread_type, std::set<pid_t>> &m_thread_pid =
      true_type_instance->m_thread_pid;

  std::atomic<bool> is_stopped;
  std::vector<pid_t> thread_pids;
  std::vector<std::thread> threads;
  start_threads(1, threads, thread_pids, is_stopped);

  auto pid = thread_pids[0];
  m_thread_pid[Thread_type::FOREGROUND].insert(pid);

  bool result = true_type_instance->bind_to_group(pid);
  ASSERT_TRUE(result);
  std::map<Thread_type, std::vector<Sched_affinity_group>> &
      m_sched_affinity_groups = true_type_instance->m_sched_affinity_groups;
  auto sched_affinity_groups =
      m_sched_affinity_groups.at(Thread_type::FOREGROUND);
  Bitmask_shared_ptr bitmask = get_bitmask_shared_ptr(numa_allocate_cpumask());
  numa_sched_getaffinity(pid, bitmask.get());
  std::map<pid_t, int> &pid_group_id = true_type_instance->m_pid_group_id;
  ASSERT_NE(pid_group_id.end(), pid_group_id.find(pid));
  auto group_id = pid_group_id.at(pid);
  ASSERT_TRUE(group_id >= 0 &&
              group_id < static_cast<int>(sched_affinity_groups.size()));
  ASSERT_GE(sched_affinity_groups[group_id].avail_cpu_num, 1);
  ASSERT_EQ(1, sched_affinity_groups[group_id].assigned_thread_num);
  ASSERT_EQ(1, numa_bitmask_equal(
                   sched_affinity_groups[group_id].avail_cpu_mask.get(), bitmask.get()));

  is_stopped.store(true);
  std::for_each(threads.begin(), threads.end(),
                [](std::thread &thread) { thread.join(); });

  Sched_affinity_manager::free_instance();
}

TEST_F(SchedAffinityManagerTest, UnbindFromGroup) {
  using namespace std::chrono_literals;

  if (skip_if_numa_unavailable()) {
    return;
  }

  std::map<sched_affinity::Thread_type, const char *> config = {
      {sched_affinity::Thread_type::FOREGROUND, "1-3"}};

  auto instance = Sched_affinity_manager::create_instance(config, false);
  auto true_type_instance =
      dynamic_cast<Sched_affinity_manager_numa *>(instance);

  std::map<Thread_type, std::set<pid_t>> &m_thread_pid =
      true_type_instance->m_thread_pid;

  std::atomic<bool> is_stopped;
  std::vector<pid_t> thread_pids;
  std::vector<std::thread> threads;
  start_threads(1, threads, thread_pids, is_stopped);

  auto pid = thread_pids[0];
  m_thread_pid[Thread_type::FOREGROUND].insert(pid);

  true_type_instance->bind_to_group(pid);
  std::map<Thread_type, std::vector<Sched_affinity_group>>
      &m_sched_affinity_groups = true_type_instance->m_sched_affinity_groups;
  auto &sched_affinity_groups =
      m_sched_affinity_groups.at(Thread_type::FOREGROUND);
  std::map<pid_t, int> &pid_group_id = true_type_instance->m_pid_group_id;

  ASSERT_FALSE(pid_group_id.empty());
  bool result = true_type_instance->unbind_from_group(pid);
  ASSERT_TRUE(result);
  ASSERT_TRUE(pid_group_id.empty());
  for (auto &group : sched_affinity_groups) {
    ASSERT_EQ(0, group.assigned_thread_num);
  }

  is_stopped.store(true);
  std::for_each(threads.begin(), threads.end(),
                [](std::thread &thread) { thread.join(); });

  Sched_affinity_manager::free_instance();
}

TEST_F(SchedAffinityManagerTest, GetThreadTypeByPid) {
  if (skip_if_numa_unavailable()) {
    return;
  }

  std::map<sched_affinity::Thread_type, const char *> config = {
      {sched_affinity::Thread_type::FOREGROUND, "1-3"}};

  auto instance = Sched_affinity_manager::create_instance(config, false);
  auto true_type_instance =
      dynamic_cast<Sched_affinity_manager_numa *>(instance);

  std::map<Thread_type, std::set<pid_t>> &m_thread_pid =
      true_type_instance->m_thread_pid;

  auto pid = gettid();
  for (auto thread_type : thread_types) {
    ASSERT_TRUE(m_thread_pid[thread_type].empty());
    m_thread_pid[thread_type].insert(pid);
    ASSERT_EQ(thread_type, true_type_instance->get_thread_type_by_pid(pid));
    m_thread_pid[thread_type].erase(pid);
    ASSERT_TRUE(m_thread_pid[thread_type].empty());
  }

  Sched_affinity_manager::free_instance();
}

TEST_F(SchedAffinityManagerTest, CheckCpuString) {
  if (skip_if_numa_unavailable()) {
    return;
  }
  auto instance = Sched_affinity_manager::create_instance(default_config, false);
  ASSERT_TRUE(instance->check_cpu_string(""));
  ASSERT_TRUE(instance->check_cpu_string(" "));
  ASSERT_TRUE(instance->check_cpu_string("  "));
  ASSERT_TRUE(instance->check_cpu_string("1"));
  ASSERT_TRUE(instance->check_cpu_string("1,2"));
  ASSERT_TRUE(instance->check_cpu_string("1-3"));
  ASSERT_TRUE(instance->check_cpu_string("1,2,3-4"));
  ASSERT_TRUE(instance->check_cpu_string(" 1  ,2, 3- 4 "));
  ASSERT_FALSE(instance->check_cpu_string(",,2, 3- 4 "));
  ASSERT_FALSE(instance->check_cpu_string("3--4"));
  ASSERT_FALSE(instance->check_cpu_string("3,,4"));
  ASSERT_FALSE(instance->check_cpu_string("3,4,"));
  ASSERT_FALSE(instance->check_cpu_string("3,4,,"));
  ASSERT_FALSE(instance->check_cpu_string(",3,4"));
  ASSERT_FALSE(instance->check_cpu_string("3-4?"));
  ASSERT_FALSE(instance->check_cpu_string("3-?4"));
  ASSERT_FALSE(instance->check_cpu_string("3?4"));
  ASSERT_FALSE(instance->check_cpu_string("?"));

  Sched_affinity_manager::free_instance();
}

TEST_F(SchedAffinityManagerTest, RegisterThread) {
  if (skip_if_numa_unavailable()) {
    return;
  }

  std::map<sched_affinity::Thread_type, const char *> config = {
      {sched_affinity::Thread_type::FOREGROUND, "1-3"}};

  auto instance = Sched_affinity_manager::create_instance(config, false);
  auto true_type_instance =
      dynamic_cast<Sched_affinity_manager_numa *>(instance);

  std::atomic<bool> is_stopped;
  std::vector<pid_t> thread_pids;
  std::vector<std::thread> threads;
  int thread_number = 5;
  start_threads(thread_number, threads, thread_pids, is_stopped);

  std::map<Thread_type, std::set<pid_t>> &m_thread_pid =
      true_type_instance->m_thread_pid;

  std::map<pid_t, int> &pid_group_id = true_type_instance->m_pid_group_id;

  for (auto &thread_pid : thread_pids) {
    instance->register_thread(Thread_type::FOREGROUND, thread_pid);
  }

  ASSERT_EQ(thread_number, m_thread_pid[Thread_type::FOREGROUND].size());
  ASSERT_EQ(thread_number, pid_group_id.size());
  ASSERT_TRUE(std::all_of(pid_group_id.begin(), pid_group_id.end(),
                          [](auto &kv) { return kv.second >= 0; }));

  is_stopped.store(true);
  std::for_each(threads.begin(), threads.end(),
                [](std::thread &thread) { thread.join(); });

  Sched_affinity_manager::free_instance();
}

TEST_F(SchedAffinityManagerTest, UnregisterThread) {
  if (skip_if_numa_unavailable()) {
    return;
  }

  std::map<sched_affinity::Thread_type, const char *> config = {
      {sched_affinity::Thread_type::FOREGROUND, "1-3"}};

  auto instance = Sched_affinity_manager::create_instance(config, false);
  auto true_type_instance =
      dynamic_cast<Sched_affinity_manager_numa *>(instance);

  std::atomic<bool> is_stopped;
  std::vector<pid_t> thread_pids;
  std::vector<std::thread> threads;
  int thread_number = 5;
  start_threads(thread_number, threads, thread_pids, is_stopped);

  std::map<Thread_type, std::set<pid_t>> &m_thread_pid =
      true_type_instance->m_thread_pid;

  std::map<pid_t, int> &pid_group_id = true_type_instance->m_pid_group_id;

  for (auto &thread_pid : thread_pids) {
    instance->register_thread(Thread_type::FOREGROUND, thread_pid);
  }

  for (auto &thread_pid : thread_pids) {
    instance->unregister_thread(thread_pid);
  }

  ASSERT_EQ(0, m_thread_pid[Thread_type::FOREGROUND].size());
  ASSERT_EQ(0, pid_group_id.size());

  is_stopped.store(true);
  std::for_each(threads.begin(), threads.end(),
                [](std::thread &thread) { thread.join(); });

  Sched_affinity_manager::free_instance();
}

TEST_F(SchedAffinityManagerTest, NumaAwareDisabled) {
  if (skip_if_numa_unavailable()) {
    return;
  }

  ASSERT_GT(test_process_node_num, 1);

  int cpu0_index = 0;
  int cpu1_index = cpu_num_per_node;
  std::string cpu_string =
      std::to_string(cpu0_index) + "," + std::to_string(cpu1_index);

  std::map<sched_affinity::Thread_type, const char *> config = {
      {sched_affinity::Thread_type::FOREGROUND, cpu_string.c_str()}};

  auto instance = Sched_affinity_manager::create_instance(config, false);
  auto true_type_instance =
      dynamic_cast<Sched_affinity_manager_numa *>(instance);

  const auto &m_sched_affinity_groups =
      true_type_instance->m_sched_affinity_groups;

  std::atomic<bool> is_stopped;
  std::vector<pid_t> thread_pids;
  std::vector<std::thread> threads;
  int thread_number = 5;
  start_threads(thread_number, threads, thread_pids, is_stopped);

  for (auto &thread_pid : thread_pids) {
    instance->register_thread(Thread_type::FOREGROUND, thread_pid);
  }

  ASSERT_EQ(1, m_sched_affinity_groups.at(Thread_type::FOREGROUND).size());
  ASSERT_EQ(2,
            m_sched_affinity_groups.at(Thread_type::FOREGROUND)[0].avail_cpu_num);
  ASSERT_EQ(
      thread_number,
      m_sched_affinity_groups.at(Thread_type::FOREGROUND)[0].assigned_thread_num);

  is_stopped.store(true);
  std::for_each(threads.begin(), threads.end(),
                [](std::thread &thread) { thread.join(); });
  Sched_affinity_manager::free_instance();
}

TEST_F(SchedAffinityManagerTest, NumaAwareEnabled) {
  if (skip_if_numa_unavailable()) {
    return;
  }

  ASSERT_GT(test_process_node_num, 1);

  int cpu0_index = 0;
  int cpu1_index = cpu_num_per_node;
  std::string cpu_string =
      std::to_string(cpu0_index) + "," + std::to_string(cpu0_index + 1) + "," +
      std::to_string(cpu1_index) + "," + std::to_string(cpu1_index + 1);

  std::map<sched_affinity::Thread_type, const char *> config = {
      {sched_affinity::Thread_type::FOREGROUND, cpu_string.c_str()}};

  auto instance = Sched_affinity_manager::create_instance(config, true);
  auto true_type_instance =
      dynamic_cast<Sched_affinity_manager_numa *>(instance);

  std::map<Thread_type, std::vector<Sched_affinity_group>>
      &m_sched_affinity_groups = true_type_instance->m_sched_affinity_groups;

  std::atomic<bool> is_stopped;
  std::vector<pid_t> thread_pids;
  std::vector<std::thread> threads;
  int thread_number = 8;
  start_threads(thread_number, threads, thread_pids, is_stopped);

  for (auto &thread_pid : thread_pids) {
    instance->register_thread(Thread_type::FOREGROUND, thread_pid);
  }

  ASSERT_EQ(test_process_node_num,
            m_sched_affinity_groups.at(Thread_type::FOREGROUND).size());
  ASSERT_EQ(2,
            m_sched_affinity_groups.at(Thread_type::FOREGROUND)[0].avail_cpu_num);
  ASSERT_EQ(2,
            m_sched_affinity_groups.at(Thread_type::FOREGROUND)[1].avail_cpu_num);
  ASSERT_EQ(
      thread_number / 2,
      m_sched_affinity_groups.at(Thread_type::FOREGROUND)[0].assigned_thread_num);
  ASSERT_EQ(
      thread_number / 2,
      m_sched_affinity_groups.at(Thread_type::FOREGROUND)[1].assigned_thread_num);

  is_stopped.store(true);
  std::for_each(threads.begin(), threads.end(),
                [](std::thread &thread) { thread.join(); });
  Sched_affinity_manager::free_instance();
}

TEST_F(SchedAffinityManagerTest, RebalanceGroup) {
  if (skip_if_numa_unavailable()) {
    return;
  }

  ASSERT_GT(test_process_node_num, 1);

  int cpu0_index = 0;
  int cpu1_index = cpu_num_per_node;
  std::string cpu_string =
      std::to_string(cpu0_index) + "," + std::to_string(cpu0_index + 1) + "," +
      std::to_string(cpu1_index) + "," + std::to_string(cpu1_index + 1);

  std::map<sched_affinity::Thread_type, const char *> config = {
      {sched_affinity::Thread_type::FOREGROUND, cpu_string.c_str()}};

  auto instance = Sched_affinity_manager::create_instance(config, true);
  auto true_type_instance =
      dynamic_cast<Sched_affinity_manager_numa *>(instance);

  const auto &m_sched_affinity_groups =
      true_type_instance->m_sched_affinity_groups;

  std::atomic<bool> is_stopped;
  std::vector<pid_t> thread_pids;
  std::vector<std::thread> threads;
  int thread_number = 8;
  start_threads(thread_number, threads, thread_pids, is_stopped);

  for (auto &thread_pid : thread_pids) {
    instance->register_thread(Thread_type::FOREGROUND, thread_pid);
  }

  std::string new_cpu_string = std::to_string(cpu0_index) + "," +
                               std::to_string(cpu1_index) + "-" +
                               std::to_string(cpu1_index + 2);
  instance->rebalance_group(new_cpu_string.c_str(), Thread_type::FOREGROUND);

  ASSERT_EQ(test_process_node_num, m_sched_affinity_groups.at(Thread_type::FOREGROUND).size());
  ASSERT_EQ(1,
            m_sched_affinity_groups.at(Thread_type::FOREGROUND)[0].avail_cpu_num);
  ASSERT_EQ(3,
            m_sched_affinity_groups.at(Thread_type::FOREGROUND)[1].avail_cpu_num);
  ASSERT_EQ(
      2,
      m_sched_affinity_groups.at(Thread_type::FOREGROUND)[0].assigned_thread_num);
  ASSERT_EQ(
      6,
      m_sched_affinity_groups.at(Thread_type::FOREGROUND)[1].assigned_thread_num);

  new_cpu_string =
      std::to_string(cpu0_index) + "-" + std::to_string(cpu0_index + 3);
  instance->rebalance_group(new_cpu_string.c_str(), Thread_type::FOREGROUND);

  ASSERT_EQ(test_process_node_num, m_sched_affinity_groups.at(Thread_type::FOREGROUND).size());
  ASSERT_EQ(4,
            m_sched_affinity_groups.at(Thread_type::FOREGROUND)[0].avail_cpu_num);
  ASSERT_EQ(0,
            m_sched_affinity_groups.at(Thread_type::FOREGROUND)[1].avail_cpu_num);
  ASSERT_EQ(
      8,
      m_sched_affinity_groups.at(Thread_type::FOREGROUND)[0].assigned_thread_num);
  ASSERT_EQ(
      0,
      m_sched_affinity_groups.at(Thread_type::FOREGROUND)[1].assigned_thread_num);

  new_cpu_string = "";
  instance->rebalance_group(new_cpu_string.c_str(), Thread_type::FOREGROUND);

  ASSERT_FALSE(
      true_type_instance->is_thread_sched_enabled(Thread_type::FOREGROUND));

  is_stopped.store(true);
  std::for_each(threads.begin(), threads.end(),
                [](std::thread &thread) { thread.join(); });
  Sched_affinity_manager::free_instance();
}

TEST_F(SchedAffinityManagerTest, TakeGroupSnapshot) {
  if (skip_if_numa_unavailable()) {
    return;
  }
  std::map<sched_affinity::Thread_type, const char *> config = {};

  auto instance = Sched_affinity_manager::create_instance(config, true);

  std::string snapshot = instance->take_group_snapshot();
  ASSERT_EQ("", snapshot);

  instance->rebalance_group("0-1", Thread_type::FOREGROUND);
  snapshot = instance->take_group_snapshot();
  ASSERT_TRUE(snapshot.find(thread_type_names.at(Thread_type::FOREGROUND)) !=
              string::npos);

  instance->rebalance_group("2-3", Thread_type::LOG_WRITER);
  snapshot = instance->take_group_snapshot();
  ASSERT_TRUE(snapshot.find(thread_type_names.at(Thread_type::FOREGROUND)) !=
              string::npos);
  ASSERT_TRUE(snapshot.find(thread_type_names.at(Thread_type::LOG_WRITER)) !=
              string::npos);

  instance->rebalance_group("", Thread_type::LOG_WRITER);
  snapshot = instance->take_group_snapshot();
  ASSERT_TRUE(snapshot.find(thread_type_names.at(Thread_type::LOG_WRITER)) ==
              string::npos);

  instance->rebalance_group("", Thread_type::FOREGROUND);
  snapshot = instance->take_group_snapshot();
  ASSERT_TRUE(snapshot.find(thread_type_names.at(Thread_type::FOREGROUND)) ==
              string::npos);

  Sched_affinity_manager::free_instance();
}

TEST_F(SchedAffinityManagerTest, UpdateNumaAware) {
  if (skip_if_numa_unavailable()) {
    return;
  }

  ASSERT_GT(test_process_node_num, 1);

  int cpu0_index = 0;
  int cpu1_index = cpu_num_per_node;
  std::string cpu_string =
      std::to_string(cpu0_index) + "-" + std::to_string(cpu0_index + 1) + "," +
      std::to_string(cpu1_index) + "-" + std::to_string(cpu1_index + 1);

  std::map<sched_affinity::Thread_type, const char *> config = {
      {sched_affinity::Thread_type::FOREGROUND, cpu_string.c_str()}};

  auto instance = Sched_affinity_manager::create_instance(config, true);
  auto true_type_instance =
      dynamic_cast<Sched_affinity_manager_numa *>(instance);

  const auto &m_sched_affinity_groups =
      true_type_instance->m_sched_affinity_groups;
  const auto &m_pid_group_id = true_type_instance->m_pid_group_id;

  std::atomic<bool> is_stopped;
  std::vector<pid_t> thread_pids;
  std::vector<std::thread> threads;
  int thread_number = 8;
  start_threads(thread_number, threads, thread_pids, is_stopped);

  for (auto &thread_pid : thread_pids) {
    instance->register_thread(Thread_type::FOREGROUND, thread_pid);
  }

  auto &sched_affinity_group = m_sched_affinity_groups.at(Thread_type::FOREGROUND);

  ASSERT_TRUE(instance->update_numa_aware(true));
  ASSERT_TRUE(instance->update_numa_aware(false));
  ASSERT_EQ(1, sched_affinity_group.size());
  ASSERT_EQ(thread_number, m_pid_group_id.size());
  ASSERT_EQ(thread_number, sched_affinity_group[0].assigned_thread_num);
  ASSERT_TRUE(std::any_of(
      m_pid_group_id.begin(), m_pid_group_id.end(),
      [](const auto & pid_group_id) { return pid_group_id.second == 0; }));
  ASSERT_TRUE(instance->update_numa_aware(false));

  ASSERT_TRUE(instance->update_numa_aware(true));
  ASSERT_EQ(test_process_node_num, sched_affinity_group.size());
  ASSERT_EQ(thread_number, m_pid_group_id.size());
  ASSERT_TRUE(std::any_of(
      m_pid_group_id.begin(), m_pid_group_id.end(),
      [](const auto & pid_group_id) {
        return pid_group_id.second == 0 || pid_group_id.second == 1;
      }));
  for (auto i = 0; i < 2; ++i) {
    ASSERT_EQ(2, sched_affinity_group[i].avail_cpu_num);
    ASSERT_EQ(4, sched_affinity_group[i].assigned_thread_num);
  }
  for (auto i = 2; i < test_process_node_num; ++i) {
    ASSERT_EQ(0, sched_affinity_group[i].avail_cpu_num);
    ASSERT_EQ(0, sched_affinity_group[i].assigned_thread_num);
  }

  is_stopped.store(true);
  std::for_each(threads.begin(), threads.end(),
                [](std::thread &thread) { thread.join(); });
  Sched_affinity_manager::free_instance();
}

} // namespace sched_affinity

#endif /* HAVE_LIBNUMA */