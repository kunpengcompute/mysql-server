#include "gtest/gtest.h"
#include "my_config.h"
#include "sql/sched_affinity_manager.h"

#ifdef HAVE_LIBNUMA

using ::sched_affinity::Sched_affinity_manager;
using ::sched_affinity::Sched_affinity_manager_numa;
using ::sched_affinity::Thread_type;
using ::testing::TestInfo;

namespace {
bool skip_if_numa_unavailable() {
  if (numa_available() == -1) {
    SUCCEED() << "Skip test case as numa is unavailable.";
    return true;
  } else {
    return false;
  }
}
}  // namespace

class SchedAffinityManagerTest : public ::testing::Test {
 protected:
  void SetUp() {}
  void TearDown() {}
};

TEST_F(SchedAffinityManagerTest, DefaultConfig) {
  if (skip_if_numa_unavailable()) {
    return;
  }
  std::map<Thread_type, char *> default_config;
  auto instance = Sched_affinity_manager::create_instance(default_config);
  ASSERT_NE(instance, nullptr);
  ASSERT_EQ(typeid(*instance), typeid(Sched_affinity_manager_numa));

  ASSERT_TRUE(instance->get_total_node_number() > 0);
  ASSERT_TRUE(instance->get_cpu_number_per_node() > 0);
}

#endif /* HAVE_LIBNUMA */