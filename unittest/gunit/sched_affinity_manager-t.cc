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
#include <condition_variable>
#include <mutex>
#include <functional>
#include <sys/syscall.h>

#include "gtest/gtest.h"

#ifdef HAVE_LIBNUMA

using ::sched_affinity::Sched_affinity_manager;
using ::sched_affinity::Sched_affinity_manager_dummy;
using ::sched_affinity::Sched_affinity_manager_numa;
using ::sched_affinity::Thread_type;
using ::testing::TestInfo;
using namespace std;

namespace sched_affinity {

extern const std::vector<Thread_type> thread_types;

class SchedAffinityManagerTest : public ::testing::Test {
 protected:
  static void start_threads(int thread_number, std::vector<std::thread> &threads,
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

  static std::string convert_bitmask_to_string(Bitmask_shared_ptr& bitmask, int& min, int& max) {
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

  static void SetUpTestCase() {
    if (numa_available() == -1) {
      return;
    }

    test_process_cpu_num = numa_num_configured_cpus();
    test_process_node_num = numa_num_configured_nodes();

    thread_boilerplate = [](std::map<Thread_type, const char *> &config_before,
                            std::map<Thread_type, const char *> &config_after,
                            std::function<void()> op, bool numa_aware_param) {
      std::mutex mutex;
      std::condition_variable cv;

      auto instance = Sched_affinity_manager::create_instance(config_before,
                                                              numa_aware_param);
      ASSERT_NE(instance, nullptr);

      std::atomic<int> times(0);
      std::vector<std::thread> threads;
      for (int i = 0; i < SchedAffinityManagerTest::test_process_node_num + 1;
           ++i) {
        threads.push_back(std::thread([op, &times, &mutex, &cv]() {
          std::unique_lock<std::mutex> thread_lock(mutex);
          bool thread_ret =
              Sched_affinity_manager::get_instance()->register_thread(
                  Thread_type::FOREGROUND, sched_affinity::gettid());
          EXPECT_EQ(thread_ret, true);
          times.store(times + 1);
          cv.wait(thread_lock);
          op();
          thread_ret =
              Sched_affinity_manager::get_instance()->unregister_thread(
                  sched_affinity::gettid());
          EXPECT_EQ(thread_ret, true);
        }));
      }

      while (times != test_process_node_num + 1) {
        sleep(0);
      }
      bool ret = Sched_affinity_manager::get_instance()->rebalance_group(
          config_after[Thread_type::FOREGROUND], Thread_type::FOREGROUND);
      EXPECT_EQ(ret, true);
      cv.notify_all();
      std::for_each(threads.begin(), threads.end(),
                    [](std::thread &thread) { thread.join(); });
      threads.clear();
      Sched_affinity_manager::free_instance();
    };

    ASSERT_GT(test_process_cpu_num, 0);
    ASSERT_GT(test_process_node_num, 0);

    cpu_num_per_node = test_process_cpu_num / test_process_node_num;

    root_process_pid = sched_affinity::gettid();
    Bitmask_shared_ptr default_bitmask =
        get_bitmask_shared_ptr(numa_allocate_cpumask());
    int ret = numa_sched_getaffinity(0, default_bitmask.get());
    ASSERT_NE(ret, -1);

    cpu_range_str = convert_bitmask_to_string(default_bitmask, cpu_range_min,
                                              cpu_range_max);

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

  virtual void SetUp() {
    if (numa_available() == -1) {
      FAIL() << "numa_unavailable test failed.";
      return;
    }
  }

 protected:
  static int test_process_cpu_num;
  static int test_process_node_num;
  static int cpu_num_per_node;
  static int cpu_range_min;
  static int cpu_range_max;
  static std::string cpu_range_str;
  static std::map<Thread_type, const char *> default_config;
  static pid_t root_process_pid;
  static std::function<void(std::map<Thread_type, const char *> &,
                            std::map<Thread_type, const char *> &,
                            std::function<void()>, bool)>
      thread_boilerplate;
};

int SchedAffinityManagerTest::test_process_cpu_num;
int SchedAffinityManagerTest::test_process_node_num;
int SchedAffinityManagerTest::cpu_num_per_node;
int SchedAffinityManagerTest::cpu_range_min;
int SchedAffinityManagerTest::cpu_range_max;
std::string SchedAffinityManagerTest::cpu_range_str;
std::map<Thread_type, const char *> SchedAffinityManagerTest::default_config;
pid_t SchedAffinityManagerTest::root_process_pid;
std::function<void(std::map<Thread_type, const char *> &,
                    std::map<Thread_type, const char *> &,
                    std::function<void()>, bool)>
    SchedAffinityManagerTest::thread_boilerplate;

TEST_F(SchedAffinityManagerTest, AllNullptrConfig) {
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
  ASSERT_FALSE(instance->check_cpu_string("1-10000"));
  ASSERT_FALSE(instance->check_cpu_string("10000"));
  std::cout<<"This test is expected to take more time."<<std::endl;
  std::string extra_long_string = std::string("1,") + std::string(2 * 1024L * 1024L * 1024L, ' ') + std::string("2");
  ASSERT_TRUE(instance->check_cpu_string(extra_long_string));
  std::string long_str;
  for (auto i = 0; i < test_process_cpu_num; ++i) {
    long_str += std::to_string(i);
    if (i != test_process_cpu_num - 1) {
      long_str += ",";
    }
  }
  ASSERT_TRUE(instance->check_cpu_string(long_str));
  Sched_affinity_manager::free_instance();
}

TEST_F(SchedAffinityManagerTest, RegisterThread) {
  std::map<sched_affinity::Thread_type, const char *> config = {
      {sched_affinity::Thread_type::FOREGROUND, "1-3"}};

  auto instance = Sched_affinity_manager::create_instance(config, false);
  auto true_type_instance =
      dynamic_cast<Sched_affinity_manager_numa *>(instance);

  std::atomic<bool> is_stopped;
  std::vector<pid_t> valid_thread_pids;
  std::vector<std::thread> valid_threads;
  int thread_number = 5;
  start_threads(thread_number, valid_threads, valid_thread_pids, is_stopped);

  std::map<Thread_type, std::set<pid_t>> &m_thread_pid =
      true_type_instance->m_thread_pid;

  std::map<pid_t, int> &pid_group_id = true_type_instance->m_pid_group_id;

  for (auto &thread_pid : valid_thread_pids) {
    ASSERT_TRUE(instance->register_thread(Thread_type::FOREGROUND, thread_pid));
  }

  ASSERT_EQ(thread_number, m_thread_pid[Thread_type::FOREGROUND].size());
  ASSERT_EQ(thread_number, pid_group_id.size());
  ASSERT_TRUE(std::all_of(pid_group_id.begin(), pid_group_id.end(),
                          [](auto &kv) { return kv.second >= 0; }));

  for (auto &thread_pid : valid_thread_pids) {
    ASSERT_TRUE(instance->unregister_thread(thread_pid));
  }

  std::vector<pid_t> invalid_thread_pids;
  std::vector<std::thread> invalid_threads;
  start_threads(thread_number, invalid_threads, invalid_thread_pids, is_stopped);

  for (auto &thread_pid : invalid_thread_pids) {
    ASSERT_TRUE(instance->register_thread(Thread_type::LOG_WRITER, thread_pid));
  }

  ASSERT_EQ(thread_number, m_thread_pid[Thread_type::LOG_WRITER].size());
  ASSERT_EQ(0, pid_group_id.size());

  is_stopped.store(true);
  std::for_each(valid_threads.begin(), valid_threads.end(),
                [](std::thread &thread) { thread.join(); });
  std::for_each(invalid_threads.begin(), invalid_threads.end(),
                [](std::thread &thread) { thread.join(); });

  Sched_affinity_manager::free_instance();
}

TEST_F(SchedAffinityManagerTest, UnregisterThread) {
  std::map<sched_affinity::Thread_type, const char *> config = {
      {sched_affinity::Thread_type::FOREGROUND, "1-3"}};

  auto instance = Sched_affinity_manager::create_instance(config, false);
  auto true_type_instance =
      dynamic_cast<Sched_affinity_manager_numa *>(instance);

  std::atomic<bool> is_stopped;
  std::vector<pid_t> valid_thread_pids;
  std::vector<std::thread> valid_threads;
  int thread_number = 5;
  start_threads(thread_number, valid_threads, valid_thread_pids, is_stopped);

  std::map<Thread_type, std::set<pid_t>> &m_thread_pid =
      true_type_instance->m_thread_pid;

  std::map<pid_t, int> &pid_group_id = true_type_instance->m_pid_group_id;

  for (auto &thread_pid : valid_thread_pids) {
    ASSERT_TRUE(instance->register_thread(Thread_type::FOREGROUND, thread_pid));
  }

  for (auto &thread_pid : valid_thread_pids) {
    ASSERT_TRUE(instance->unregister_thread(thread_pid));
  }

  ASSERT_EQ(0, m_thread_pid[Thread_type::FOREGROUND].size());
  ASSERT_EQ(0, pid_group_id.size());

  is_stopped.store(true);
  std::for_each(valid_threads.begin(), valid_threads.end(),
                [](std::thread &thread) { thread.join(); });

  Sched_affinity_manager::free_instance();
}

TEST_F(SchedAffinityManagerTest, NumaAwareDisabled) {
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

  new_cpu_string = 
      std::to_string(cpu0_index) + "," + std::to_string(cpu0_index + 1) + "," +
      std::to_string(cpu1_index) + "," + std::to_string(cpu1_index + 1);
  instance->rebalance_group(new_cpu_string.c_str(), Thread_type::FOREGROUND);

  ASSERT_EQ(test_process_node_num, m_sched_affinity_groups.at(Thread_type::FOREGROUND).size());
  ASSERT_EQ(2,
            m_sched_affinity_groups.at(Thread_type::FOREGROUND)[0].avail_cpu_num);
  ASSERT_EQ(2,
            m_sched_affinity_groups.at(Thread_type::FOREGROUND)[1].avail_cpu_num);
  ASSERT_EQ(
      4,
      m_sched_affinity_groups.at(Thread_type::FOREGROUND)[0].assigned_thread_num);
  ASSERT_EQ(
      4,
      m_sched_affinity_groups.at(Thread_type::FOREGROUND)[1].assigned_thread_num);

  // Numa_aware off
  instance->update_numa_aware(false);

  new_cpu_string = std::to_string(cpu0_index) + "," +
                               std::to_string(cpu1_index) + "-" +
                               std::to_string(cpu1_index + 2);
  instance->rebalance_group(new_cpu_string.c_str(), Thread_type::FOREGROUND);

  ASSERT_EQ(1, m_sched_affinity_groups.at(Thread_type::FOREGROUND).size());
  ASSERT_EQ(4, 
            m_sched_affinity_groups.at(Thread_type::FOREGROUND)[0].avail_cpu_num);
  ASSERT_EQ(
      8,
      m_sched_affinity_groups.at(Thread_type::FOREGROUND)[0].assigned_thread_num);

  new_cpu_string =
      std::to_string(cpu0_index) + "-" + std::to_string(cpu0_index + 3);
  instance->rebalance_group(new_cpu_string.c_str(), Thread_type::FOREGROUND);

  ASSERT_EQ(1, m_sched_affinity_groups.at(Thread_type::FOREGROUND).size());
  ASSERT_EQ(4,
            m_sched_affinity_groups.at(Thread_type::FOREGROUND)[0].avail_cpu_num);
  ASSERT_EQ(
      8,
      m_sched_affinity_groups.at(Thread_type::FOREGROUND)[0].assigned_thread_num);

  new_cpu_string = "";
  instance->rebalance_group(new_cpu_string.c_str(), Thread_type::FOREGROUND);

  ASSERT_FALSE(true_type_instance->is_thread_sched_enabled(Thread_type::FOREGROUND));

  new_cpu_string = 
      std::to_string(cpu0_index) + "," + std::to_string(cpu0_index + 1) + "," +
      std::to_string(cpu1_index) + "," + std::to_string(cpu1_index + 1);
  instance->rebalance_group(new_cpu_string.c_str(), Thread_type::FOREGROUND);

  ASSERT_EQ(1, m_sched_affinity_groups.at(Thread_type::FOREGROUND).size());
  ASSERT_EQ(4,
            m_sched_affinity_groups.at(Thread_type::FOREGROUND)[0].avail_cpu_num);
  ASSERT_EQ(
      8,
      m_sched_affinity_groups.at(Thread_type::FOREGROUND)[0].assigned_thread_num);

  is_stopped.store(true);
  std::for_each(threads.begin(), threads.end(),
                [](std::thread &thread) { thread.join(); });
  Sched_affinity_manager::free_instance();
}

TEST_F(SchedAffinityManagerTest, TakeGroupSnapshot) {
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
  thread_pids.clear();
  std::for_each(threads.begin(), threads.end(),
                [](std::thread &thread) { thread.join(); });
  Sched_affinity_manager::free_instance();
}

TEST_F(SchedAffinityManagerTest, RebalanceGroupThreadAffinity) {
  std::function<void()> thread_op = nullptr;

  /*
   * This part is to test whether the reschedule-function will function as
   * expected when the foreground thread parameter from on to off, and the
   * background thread parameter is always off.
   */
  thread_op = [this]() {
    auto process_bitmask = get_bitmask_shared_ptr(numa_allocate_cpumask());
    auto thread_bitmask = get_bitmask_shared_ptr(numa_allocate_cpumask());
    ASSERT_GE(numa_sched_getaffinity(root_process_pid, process_bitmask.get()), 0);
    ASSERT_GE(numa_sched_getaffinity(sched_affinity::gettid(), thread_bitmask.get()),
              0);
    for (int j = 0; j < test_process_cpu_num; ++j) {
      bool p_ret = numa_bitmask_isbitset(process_bitmask.get(), j);
      bool t_ret = numa_bitmask_isbitset(thread_bitmask.get(), j);
      EXPECT_EQ(p_ret, t_ret);
    }
  };

  bool numa_aware_param = true;
  auto config_before = default_config;
  config_before[Thread_type::FOREGROUND] = const_cast<char *>(cpu_range_str.c_str());
  auto config_after = default_config;
  config_after[Thread_type::FOREGROUND] = nullptr;
  thread_boilerplate(config_before, config_after, thread_op, numa_aware_param);

  /*
   * This part is to test whether the reschedule-function will function as
   * expected when the foreground thread parameter from off to on, and the
   * background thread parameter is always off.
   */
  thread_op = [this]() {
    auto thread_bitmask = get_bitmask_shared_ptr(numa_allocate_cpumask());
    int bitmask_ret =
        numa_sched_getaffinity(sched_affinity::gettid(), thread_bitmask.get());
    ASSERT_GE(bitmask_ret, 0);
    for (int j = 0; j < test_process_cpu_num; ++j) {
      if (j == cpu_range_min) {
        EXPECT_EQ(numa_bitmask_isbitset(thread_bitmask.get(), j), true);
      } else {
        EXPECT_EQ(numa_bitmask_isbitset(thread_bitmask.get(), j), false);
      }
    }
  };

  numa_aware_param = true;
  config_before = default_config;
  config_before[Thread_type::FOREGROUND] = nullptr;
  config_after = default_config;
  config_after[Thread_type::FOREGROUND] =
      const_cast<char *>(std::to_string(cpu_range_min).c_str());
  thread_boilerplate(config_before, config_after, thread_op, numa_aware_param);

  /*
   * This part is to test whether the reschedule-function will function as 
   * expected when the foreground thread parameter changed, and the 
   * background thread parameter is always off.
   */
  thread_op = [this]() {
    auto thread_bitmask = get_bitmask_shared_ptr(numa_allocate_cpumask());
    int bitmask_ret =
        numa_sched_getaffinity(sched_affinity::gettid(), thread_bitmask.get());
    ASSERT_GE(bitmask_ret, 0);
    bool is_target_index_set;
    for (int j = 0; j < test_process_cpu_num; ++j) {
      is_target_index_set = numa_bitmask_isbitset(thread_bitmask.get(), j);
      if (j == cpu_range_min) {
        EXPECT_EQ(is_target_index_set, true);
      } else {
        EXPECT_EQ(is_target_index_set, false);
      }
    }
  };

  numa_aware_param = true;
  config_before = default_config;
  config_before[Thread_type::FOREGROUND] =
      const_cast<char *>(cpu_range_str.c_str());
  config_after = default_config;
  config_after[Thread_type::FOREGROUND] =
      const_cast<char *>(std::to_string(cpu_range_min).c_str());
  thread_boilerplate(config_before, config_after, thread_op, numa_aware_param);
}

TEST_F(SchedAffinityManagerTest, RebalanceGroupOptional) {
  if (test_process_node_num < 4) {
    FAIL()<<"test_process_node_num < 4";
    return;
  }

  std::function<void()> thread_op = nullptr;

  int cpu0_index = 0;
  int cpu1_index = 1 * cpu_num_per_node;
  int cpu2_index = 2 * cpu_num_per_node;
  int cpu3_index = 3 * cpu_num_per_node;
  int cpu4_index = 4 * cpu_num_per_node;

  /*
   * This test case collection is to offer more test cases for the rebalance_group 
   * function when the cpu range changed.
   * The scenarios contains:
   * 1. one node used before the rebalance, and three nodes after.
   * 2. two nodes whithin same socket used before the rebalance, and two other after.
   * 3. two nodes at different sockets used before the rebalance, and two other after.
   * 4. three nodes used before the rebalance, and one node after.
   */
  thread_op = [this]() {
    auto thread_bitmask = get_bitmask_shared_ptr(numa_allocate_cpumask());
    int bitmask_ret =
        numa_sched_getaffinity(sched_affinity::gettid(), thread_bitmask.get());
    ASSERT_GE(bitmask_ret, 0);
    bool is_target_index_set;
    for (int j = 0; j < test_process_cpu_num; ++j) {
      is_target_index_set = numa_bitmask_isbitset(thread_bitmask.get(), j);
      if (j >= (test_process_node_num - 2) * cpu_num_per_node &&
          j < (test_process_node_num - 1) * cpu_num_per_node) {
        EXPECT_EQ(is_target_index_set, false);
      }
    }
  };

  bool numa_aware_param = true;
  auto config_before = default_config;
  std::string cpu_string =
      std::to_string(cpu2_index) + "-" + std::to_string(cpu3_index - 1);
  config_before[Thread_type::FOREGROUND] = cpu_string.c_str();
  auto config_after = default_config;
  std::string new_cpu_string =
      std::to_string(cpu0_index) + "-" + std::to_string(cpu2_index - 1) + "," +
      std::to_string(cpu3_index) + "-" + std::to_string(cpu4_index - 1);
  config_after[Thread_type::FOREGROUND] = new_cpu_string.c_str();
  thread_boilerplate(config_before, config_after, thread_op, numa_aware_param);


  thread_op = [this]() {
    auto thread_bitmask = get_bitmask_shared_ptr(numa_allocate_cpumask());
    int bitmask_ret =
        numa_sched_getaffinity(sched_affinity::gettid(), thread_bitmask.get());
    ASSERT_GE(bitmask_ret, 0);
    bool is_target_index_set;
    for (int j = 0; j < test_process_cpu_num; ++j) {
      is_target_index_set = numa_bitmask_isbitset(thread_bitmask.get(), j);
      if (j >= 0 && j < (test_process_node_num - 2) * cpu_num_per_node) {
        EXPECT_EQ(is_target_index_set, false);
      }
    }
  };

  numa_aware_param = true;
  config_before = default_config;
  cpu_string =
      std::to_string(cpu0_index) + "-" + std::to_string(cpu2_index - 1);
  config_before[Thread_type::FOREGROUND] = cpu_string.c_str();
  config_after = default_config;
  new_cpu_string =
      std::to_string(cpu2_index) + "-" + std::to_string(cpu4_index - 1);
  config_after[Thread_type::FOREGROUND] = new_cpu_string.c_str();
  thread_boilerplate(config_before, config_after, thread_op, numa_aware_param);

  thread_op = [this]() {
    auto thread_bitmask = get_bitmask_shared_ptr(numa_allocate_cpumask());
    int bitmask_ret =
        numa_sched_getaffinity(sched_affinity::gettid(), thread_bitmask.get());
    ASSERT_GE(bitmask_ret, 0);
    bool is_target_index_set;
    for (int j = 0; j < test_process_cpu_num; ++j) {
      is_target_index_set = numa_bitmask_isbitset(thread_bitmask.get(), j);
      if ((j >= 0 && j < (test_process_node_num - 3) * cpu_num_per_node) ||
          (j >= (test_process_node_num - 2) * cpu_num_per_node &&
           j < (test_process_node_num - 1) * cpu_num_per_node)) {
        EXPECT_EQ(is_target_index_set, false);
      }
    }
  };

  numa_aware_param = true;
  config_before = default_config;
  cpu_string =
      std::to_string(cpu0_index) + "-" + std::to_string(cpu1_index - 1) + "," +
      std::to_string(cpu2_index) + "-" + std::to_string(cpu3_index - 1);
  config_before[Thread_type::FOREGROUND] = cpu_string.c_str();
  config_after = default_config;
  new_cpu_string =
      std::to_string(cpu1_index) + "-" + std::to_string(cpu2_index - 1) + "," +
      std::to_string(cpu3_index) + "-" + std::to_string(cpu4_index - 1);
  config_after[Thread_type::FOREGROUND] = new_cpu_string.c_str();
  thread_boilerplate(config_before, config_after, thread_op, numa_aware_param);

  thread_op = [this]() {
    auto thread_bitmask = get_bitmask_shared_ptr(numa_allocate_cpumask());
    int bitmask_ret =
        numa_sched_getaffinity(sched_affinity::gettid(), thread_bitmask.get());
    ASSERT_GE(bitmask_ret, 0);
    bool is_target_index_set;
    for (int j = 0; j < test_process_cpu_num; ++j) {
      is_target_index_set = numa_bitmask_isbitset(thread_bitmask.get(), j);
      if (j >= (test_process_node_num - 2) * cpu_num_per_node &&
          j < (test_process_node_num - 1) * cpu_num_per_node) {
        EXPECT_EQ(is_target_index_set, true);
      } else {
        EXPECT_EQ(is_target_index_set, false);
      }
    }
  };

  numa_aware_param = true;
  config_before = default_config;
  cpu_string =
      std::to_string(cpu0_index) + "-" + std::to_string(cpu2_index - 1) + "," +
      std::to_string(cpu3_index) + "-" + std::to_string(cpu4_index - 1);
  config_before[Thread_type::FOREGROUND] = cpu_string.c_str();
  config_after = default_config;
  new_cpu_string =
      std::to_string(cpu2_index) + "-" + std::to_string(cpu3_index - 1);
  config_after[Thread_type::FOREGROUND] = new_cpu_string.c_str();
  thread_boilerplate(config_before, config_after, thread_op, numa_aware_param);
}

TEST_F(SchedAffinityManagerTest, GetTotalNodeNumber) {
  auto instance = Sched_affinity_manager::create_instance(default_config, true);
  ASSERT_NE(instance, nullptr);

  int criterion = numa_num_configured_nodes();
  EXPECT_EQ(Sched_affinity_manager::get_instance()->get_total_node_number(), criterion);

  Sched_affinity_manager::free_instance();
}

TEST_F(SchedAffinityManagerTest, GetCpuNumberPerNode) {
  auto instance = Sched_affinity_manager::create_instance(default_config, true);
  ASSERT_NE(instance, nullptr);

  auto criterion = numa_num_configured_cpus() / numa_num_configured_nodes();
  EXPECT_EQ(Sched_affinity_manager::get_instance()->get_cpu_number_per_node(), criterion);

  Sched_affinity_manager::free_instance();
}

} // namespace sched_affinity

#endif /* HAVE_LIBNUMA */