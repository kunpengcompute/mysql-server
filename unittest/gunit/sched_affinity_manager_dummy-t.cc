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

#include "gtest/gtest.h"
#include "sql/sched_affinity_manager.h"

namespace sched_affinity {

class SchedAffinityManagerDummyTest : public ::testing::Test {};

TEST_F(SchedAffinityManagerDummyTest, Implementation) {
  std::map<Thread_type, const char *> default_config = {
      {sched_affinity::Thread_type::FOREGROUND, nullptr},
      {sched_affinity::Thread_type::LOG_WRITER, nullptr},
      {sched_affinity::Thread_type::LOG_FLUSHER, nullptr},
      {sched_affinity::Thread_type::LOG_WRITE_NOTIFIER, nullptr},
      {sched_affinity::Thread_type::LOG_FLUSH_NOTIFIER, nullptr},
      {sched_affinity::Thread_type::LOG_CLOSER, nullptr},
      {sched_affinity::Thread_type::LOG_CHECKPOINTER, nullptr},
      {sched_affinity::Thread_type::PURGE_COORDINATOR, nullptr}};
  auto *sched_affinity_manager = new Sched_affinity_manager_dummy();
  ASSERT_TRUE(sched_affinity_manager->init(default_config, false));
  ASSERT_TRUE(
      sched_affinity_manager->register_thread(Thread_type::FOREGROUND, 0));
  ASSERT_TRUE(
      sched_affinity_manager->register_thread(Thread_type::LOG_WRITER, 0));
  ASSERT_TRUE(sched_affinity_manager->unregister_thread(0));
  ASSERT_TRUE(
      sched_affinity_manager->rebalance_group("", Thread_type::FOREGROUND));
  ASSERT_TRUE(
      sched_affinity_manager->rebalance_group("", Thread_type::LOG_WRITER));
  ASSERT_TRUE(sched_affinity_manager->update_numa_aware(true));
  ASSERT_TRUE(sched_affinity_manager->update_numa_aware(false));
  ASSERT_TRUE(sched_affinity_manager->take_group_snapshot().empty());
  ASSERT_EQ(-1, sched_affinity_manager->get_total_node_number());
  ASSERT_EQ(-1, sched_affinity_manager->get_cpu_number_per_node());
  std::string cpu_string = "";
  ASSERT_TRUE(sched_affinity_manager->check_cpu_string(cpu_string));
  delete sched_affinity_manager;
}

}  // namespace sched_affinity