#include "sql/sched_affinity_manager.h"
#ifdef HAVE_LIBNUMA
#include <numa.h>
#endif
#include <string>
#include <iostream>
#include <thread>
#include "gtest/gtest.h"
#include "my_config.h"

#ifdef HAVE_LIBNUMA

using ::sched_affinity::Sched_affinity_manager;
using ::sched_affinity::Sched_affinity_manager_numa;
using ::sched_affinity::Thread_type;
using ::testing::TestInfo;
using namespace std;

const Thread_type thread_types[] = {
    Thread_type::FOREGROUND,         Thread_type::LOG_WRITER,
    Thread_type::LOG_FLUSHER,        Thread_type::LOG_WRITE_NOTIFIER,
    Thread_type::LOG_FLUSH_NOTIFIER, Thread_type::LOG_CLOSER,
    Thread_type::LOG_CHECKPOINTER,   Thread_type::PURGE_COORDINATOR};

class SchedAffinityManagerTest : public ::testing::Test {
 protected:
  bool skip_if_numa_unavailable() {
    if (numa_available() == -1) {
      SUCCEED() << "Skip test case as numa is unavailable.";
      return true;
    } else {
      return false;
    }
  }

  std::string convert_bitmask_to_string(struct bitmask* bitmask, int& min, int& max) {
    std::string res;

    int i = 0, total_cpu_num = numa_num_configured_cpus();
    bool flag1 = false, flag2 = false;
    for (; i < total_cpu_num; ) {
      int start = i, curr = i;

      while (curr < total_cpu_num 
             && numa_bitmask_isbitset(bitmask, curr)) {
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
    struct bitmask *sched_bitmask = numa_allocate_cpumask();
    int ret = numa_sched_getaffinity(0, sched_bitmask);

    if (ret != -1) {
      bool flag = false;
      for (int i = 0; i < numa_num_configured_cpus(); i++) {
        if ((i == index && !numa_bitmask_isbitset(sched_bitmask, i)) 
            || (i != index && numa_bitmask_isbitset(sched_bitmask, i))) {
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

    numa_free_cpumask(sched_bitmask);
    sched_bitmask = nullptr;
    return true;
  }

  int get_avail_node_num(int node_num) {
    int res = 0;
    struct bitmask *bm = numa_get_run_node_mask();

    for (int i = 0; i < node_num; i++) {
      if (numa_bitmask_isbitset(bm, i)) {
        res++;
      }
    }

    return res;
  }

  void init_avail_cpu_num_per_node(std::vector<int> &avail_cpu_num_per_node, int cpu_num_per_node, 
                                   struct bitmask *default_bitmask) {
    for (int i = 0; i < test_process_node_num; i++) {
      int count = 0;
      for (int j = i * cpu_num_per_node; j < (i + 1) * cpu_num_per_node; j++) {
        if (numa_bitmask_isbitset(default_bitmask, j)) {
          count++;
        }
      }
      avail_cpu_num_per_node[i] = count;
    }
  }

  void init_avail_nodes_arr(std::vector<char> &avail_nodes_arr, int test_process_node_num) {
    struct bitmask *test_process_node_mask = numa_get_run_node_mask();

    for (int i = 0, arr_index = 0; i < test_process_node_num; i++) {
      if (numa_bitmask_isbitset(test_process_node_mask, i)) {
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

      
      default_bitmask = numa_allocate_cpumask();
      int ret = numa_sched_getaffinity(0, default_bitmask);
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
    numa_free_cpumask(default_bitmask);
    default_bitmask = nullptr;
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

  struct bitmask *default_bitmask;
};

TEST_F(SchedAffinityManagerTest, DefaultConfig) {
  if (skip_if_numa_unavailable()) {
    return;
  }

  auto instance = Sched_affinity_manager::create_instance(default_config);
  ASSERT_NE(instance, nullptr);
  ASSERT_EQ(typeid(*instance), typeid(Sched_affinity_manager_numa));

  ASSERT_TRUE(instance->get_total_node_number() > 0);
  ASSERT_TRUE(instance->get_cpu_number_per_node() > 0);

  Sched_affinity_manager::free_instance();
}

TEST_F(SchedAffinityManagerTest, ErrorFormatConfig) {
  if (skip_if_numa_unavailable()) {
    return;
  }

  /* Blank space at the beginning of string */
  std::string test_str = " " + cpu_range_str;
  default_config[sched_affinity::Thread_type::FOREGROUND] = test_str.c_str();

  auto instance = Sched_affinity_manager::create_instance(default_config);
  EXPECT_NE(instance, nullptr);

  Sched_affinity_manager::free_instance();

  /* Blank space at the end of string */
  test_str = cpu_range_str + " ";
  default_config[sched_affinity::Thread_type::FOREGROUND] = test_str.c_str();
  instance = Sched_affinity_manager::create_instance(default_config);
  EXPECT_EQ(instance, nullptr);

  Sched_affinity_manager::free_instance();

  /* Blank space in the middle of string */
  test_str = std::to_string(cpu_range_min) + " ," + std::to_string(cpu_range_max);
  default_config[sched_affinity::Thread_type::FOREGROUND] = test_str.c_str();
  instance = Sched_affinity_manager::create_instance(default_config);
  EXPECT_EQ(instance, nullptr);

  Sched_affinity_manager::free_instance();

  /* Cpu range cross the border */
  test_str = cpu_range_str + "," + std::to_string(cpu_range_max + 1);
  default_config[sched_affinity::Thread_type::FOREGROUND] = test_str.c_str();
  instance = Sched_affinity_manager::create_instance(default_config);
  EXPECT_EQ(instance, nullptr);

  Sched_affinity_manager::free_instance();
}

TEST_F(SchedAffinityManagerTest, ThreadProcessConflictConfig) {
  if (skip_if_numa_unavailable()) {
    return;
  }
  
  std::string test_str = std::to_string(cpu_range_min);
  struct bitmask *test_process_bitmask = numa_parse_cpustring(test_str.c_str());
  int ret = numa_sched_setaffinity(0, test_process_bitmask);
  ASSERT_NE(ret, -1);

  if (cpu_range_min != cpu_range_max) {
    test_str = std::to_string(cpu_range_max);
  } else if (cpu_range_min < test_process_cpu_num - 1) {
    test_str = std::to_string(cpu_range_min + 1);
  } else {
    test_str = std::to_string(cpu_range_min -1);
  }
  
  default_config[sched_affinity::Thread_type::FOREGROUND] = test_str.c_str();
  auto instance = Sched_affinity_manager::create_instance(default_config);
  EXPECT_EQ(instance, nullptr);

  numa_sched_setaffinity(0, default_bitmask);
  ASSERT_NE(ret, -1);

  numa_free_cpumask(test_process_bitmask);
  test_process_bitmask = nullptr;
  Sched_affinity_manager::free_instance();
}

TEST_F(SchedAffinityManagerTest, ForegroundBackgroundConflictConfig) {
  if (skip_if_numa_unavailable()) {
    return;
  }
  
  std::string test_fore_str = cpu_range_str;
  default_config[sched_affinity::Thread_type::FOREGROUND] = test_fore_str.c_str();

  std::string test_back_str = cpu_range_str;
  default_config[sched_affinity::Thread_type::LOG_WRITER] = test_back_str.c_str();

  auto instance = Sched_affinity_manager::create_instance(default_config);
  EXPECT_NE(instance, nullptr);

  Sched_affinity_manager::free_instance();
}

TEST_F(SchedAffinityManagerTest, BindToGroup) {
  if (skip_if_numa_unavailable()) {
    return;
  }
  
  std::string test_str = cpu_range_str;
  default_config[sched_affinity::Thread_type::FOREGROUND] = test_str.c_str();

  auto instance = Sched_affinity_manager::create_instance(default_config);
  ASSERT_NE(instance, nullptr);

  int *thread_num_per_node = new int[test_process_node_num];
  for (int i = 0; i < test_process_node_num; i++) {
    thread_num_per_node[i] = 0;
  }

  /* Multi thread version, threads executed in sequence. */
  for (int i = 0; i < test_process_node_num + 1; i++) {
    std::thread th([i, &thread_num_per_node, this] {
      int group_index = -1;
      int criterion_index = -1;
      EXPECT_EQ(Sched_affinity_manager::get_instance()->bind_to_group(group_index), true);
      for (int j = 0; j < test_process_node_num; j++) {
        if (avail_cpu_num_per_node[j] == 0) {
          continue;
        }
        if (criterion_index == -1
            || thread_num_per_node[j] * avail_cpu_num_per_node[criterion_index]
               < thread_num_per_node[criterion_index] * avail_cpu_num_per_node[j]) {
            criterion_index = j;
        }
      }
      thread_num_per_node[criterion_index]++;
      EXPECT_EQ(group_index, criterion_index);
      struct bitmask *bm = numa_allocate_cpumask();
      int ret = numa_sched_getaffinity(0, bm);
      EXPECT_NE(ret, -1);
      for(int j = 0; j < numa_num_configured_cpus(); j++) {
       if(j >= group_index * cpu_num_per_node
          && j < (group_index + 1) * cpu_num_per_node) {
          EXPECT_EQ(numa_bitmask_isbitset(bm, j), numa_bitmask_isbitset(default_bitmask, j));
        } else {
          EXPECT_EQ(numa_bitmask_isbitset(bm, j), 0);
        }
      }
      numa_free_cpumask(bm);
      bm = nullptr;
    });
    th.join();
  }

  std::string criterion_str;

  for (int i = 0; i < test_process_node_num; i++) {
    criterion_str += std::to_string(thread_num_per_node[i]) + "/" + std::to_string(avail_cpu_num_per_node[i]) + "; ";
  }

  std::string buffered_str;
  char *buff = new char[BUFFER_SIZE_1024];
  Sched_affinity_manager::get_instance()->take_snapshot(buff, 1024);
  buffered_str = std::string(buff);
  
  EXPECT_EQ(buffered_str, criterion_str);

  Sched_affinity_manager::free_instance();
}

TEST_F(SchedAffinityManagerTest, UnbindFromGroup) {
  if (skip_if_numa_unavailable()) {
    return;
  }
  
  std::string test_str = cpu_range_str;
  default_config[sched_affinity::Thread_type::FOREGROUND] = test_str.c_str();

  auto instance = Sched_affinity_manager::create_instance(default_config);
  ASSERT_NE(instance, nullptr);

  int group_index_nobind = -1;
  std::thread th1([&group_index_nobind] {
    EXPECT_EQ(Sched_affinity_manager::get_instance()->unbind_from_group(group_index_nobind), false);
  });
  th1.join();

  int group_index_excessive = test_process_node_num;
  std::thread th2([&group_index_excessive] {
    EXPECT_EQ(Sched_affinity_manager::get_instance()->unbind_from_group(group_index_excessive), false);
  });
  th2.join();

  int group_index = -1;
  std::thread th3([&group_index, this] {
    EXPECT_EQ(Sched_affinity_manager::get_instance()->bind_to_group(group_index), true);
    EXPECT_EQ(group_index, avail_nodes_arr[0]);
    EXPECT_EQ(Sched_affinity_manager::get_instance()->unbind_from_group(group_index), true);
  });
  th3.join();

  Sched_affinity_manager::free_instance();
}

TEST_F(SchedAffinityManagerTest, BindToTarget) {
  if (skip_if_numa_unavailable()) {
    return;
  }
  
  auto instance = Sched_affinity_manager::create_instance(default_config);
  ASSERT_NE(instance, nullptr);

  for (const auto i : thread_types) {
    std::thread th([i] {
      if (i == sched_affinity::Thread_type::FOREGROUND) {
        EXPECT_EQ(Sched_affinity_manager::get_instance()->bind_to_target(i), false);
      } else {
        EXPECT_EQ(Sched_affinity_manager::get_instance()->bind_to_target(i), true);
      }});
    th.join();
  }

  Sched_affinity_manager::free_instance();

  for (const auto i : thread_types) {
    default_config[i] = std::to_string(cpu_range_min).c_str();
  }

  instance = Sched_affinity_manager::create_instance(default_config);
  ASSERT_NE(instance, nullptr);

  EXPECT_EQ(Sched_affinity_manager::get_instance()->bind_to_target(sched_affinity::Thread_type::FOREGROUND), false);
  for (const auto i :thread_types) {
    if (i != sched_affinity::Thread_type::FOREGROUND) {
      std::thread th([i, this] {
      EXPECT_EQ(Sched_affinity_manager::get_instance()->bind_to_target(i), true);
      EXPECT_EQ(check_bind_to_target(cpu_range_min), true);});
    th.join();    
    }
  }

  Sched_affinity_manager::free_instance();
}

TEST_F(SchedAffinityManagerTest, TakeSnapshot) {
  if (skip_if_numa_unavailable()) {
    return;
  }
  
  std::string test_str = cpu_range_str;
  default_config[sched_affinity::Thread_type::FOREGROUND] = test_str.c_str();
  auto instance = Sched_affinity_manager::create_instance(default_config);
  ASSERT_NE(instance, nullptr);

  /* test the case when the buff param is a null pointer */
  char *buff = nullptr;
  int buff_size = BUFFER_SIZE_1024;

  Sched_affinity_manager::get_instance()->take_snapshot(buff, buff_size);
  EXPECT_EQ(buff, nullptr);

  buff = new char[BUFFER_SIZE_1024];

  ASSERT_NE(buff, nullptr);

  int initial_group_index = -1;
  std::thread th([&initial_group_index] {Sched_affinity_manager::get_instance()->bind_to_group(initial_group_index);});
  th.join();

  std::string criterion_str;
  std::string buffered_str;

  for (int i = 0; i < test_process_node_num; i++) {
    if (i == avail_nodes_arr[0]) {
      criterion_str += "1/" + std::to_string(avail_cpu_num_per_node[i]);
    } else {
      criterion_str += "0/" + std::to_string(avail_cpu_num_per_node[i]);
    }
    criterion_str += "; ";
  }

  /* test the case when the buff_size param is negative */
  buff_size = -2;
  Sched_affinity_manager::get_instance()->take_snapshot(buff, buff_size);
  buffered_str = std::string(buff);
  EXPECT_NE(criterion_str, buffered_str);

  /* test the case when the params are both normal*/
  buff_size = BUFFER_SIZE_1024;
  Sched_affinity_manager::get_instance()->take_snapshot(buff, buff_size);
  buffered_str = std::string(buff);
  EXPECT_EQ(criterion_str, buffered_str);

  buff = nullptr;
  Sched_affinity_manager::free_instance();
}

TEST_F(SchedAffinityManagerTest, GetTotalNodeNumber) {
  if (skip_if_numa_unavailable()) {
    return;
  }
  
  auto instance = Sched_affinity_manager::create_instance(default_config);
  ASSERT_NE(instance, nullptr);

  int criterion = numa_num_configured_nodes();
  EXPECT_EQ(Sched_affinity_manager::get_instance()->get_total_node_number(), criterion);

  Sched_affinity_manager::free_instance();
}

TEST_F(SchedAffinityManagerTest, GetCpuNumberPerNode) {
  if (skip_if_numa_unavailable()) {
    return;
  }
  
  auto instance = Sched_affinity_manager::create_instance(default_config);
  ASSERT_NE(instance, nullptr);

  auto criterion = numa_num_configured_cpus() / numa_num_configured_nodes();
  EXPECT_EQ(Sched_affinity_manager::get_instance()->get_cpu_number_per_node(), criterion);

  Sched_affinity_manager::free_instance();
}

#endif /* HAVE_LIBNUMA */