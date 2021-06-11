/* Copyright (c) 2021, Huawei and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <iostream>

#include "sql/pq_condition.h"
#include "sql/sql_class.h"
#include "sql/item_func.h"
#include "sql/item_cmpfunc.h"
#include "sql/item_sum.h"
#include "sql/sql_optimizer.h"
#include "sql/item_json_func.h"
#include "field_types.h"
#include "unittest/gunit/test_utils.h"
#include "unittest/gunit/parsertest.h"
#include "unittest/gunit/base_mock_field.h"
#include "unittest/gunit/fake_table.h"


using namespace std;
using my_testing::Server_initializer;


extern uint parallel_threads_running;
extern ulong parallel_max_threads;
extern bool pq_not_support_datatype(enum_field_types type);
extern bool pq_not_support_functype(Item_func::Functype type);
extern bool pq_not_support_func(Item_func *func);
extern bool pq_not_support_aggr_functype(Item_sum::Sumfunctype type);
extern bool pq_not_support_ref(Item_ref *ref);
extern bool check_pq_support_fieldtype_of_field_item(Item *item);
extern bool check_pq_support_fieldtype_of_func_item(Item *item);
extern bool check_pq_support_fieldtype_of_ref_item(Item *item);
extern bool check_pq_support_fieldtype_of_cache_item(Item *item);
extern bool check_pq_support_fieldtype(Item *item);
extern bool choose_parallel_scan_table(JOIN *join);
extern void set_pq_dop(THD *thd);
extern bool suite_for_parallel_query(THD *thd);
extern bool suite_for_parallel_query(JOIN *join);
extern bool check_pq_running_threads(uint dop, ulong timeout_ms);


namespace parallel_query_test {

class PqConditionTest : public ::testing::Test {
protected:
  virtual void SetUp() { initializer.SetUp(); }
  virtual void TearDown() { initializer.TearDown(); }

  Server_initializer initializer;
  MEM_ROOT m_mem_root;
  Base_mock_field_json m_field;
  Fake_TABLE m_table{&m_field};

  Item_string *new_item_string(const char *str) {
    return new Item_string(str, std::strlen(str), &my_charset_utf8mb4_bin);
  }
};

TEST_F(PqConditionTest, pq_not_support_datatype) {
  bool result;

  result = pq_not_support_datatype(MYSQL_TYPE_TINY_BLOB);
  EXPECT_EQ(true, result);

   result = pq_not_support_datatype(MYSQL_TYPE_DECIMAL);
  EXPECT_EQ(false, result);
}

TEST_F(PqConditionTest, pq_not_support_functype) {
  bool result;

  result = pq_not_support_functype(Item_func::JSON_FUNC);
  EXPECT_EQ(true, result);

   result = pq_not_support_functype(Item_func::EQ_FUNC);
  EXPECT_EQ(false, result);
}

TEST_F(PqConditionTest, pq_not_support_func) {
  bool result;
  THD *thd = initializer.thd();

  auto item1 = new Item_int(1);
  auto item2 = new Item_int(1);
  auto item3 = new Item_func_eq(item1, item2);

  result = pq_not_support_func(item3);
  EXPECT_EQ(false, result);

  auto item4 = new Item_func_release_all_locks(POS());
  result = pq_not_support_func(item4);
  EXPECT_EQ(true, result);

  auto item5 = new Item_func_json_remove(thd, new Item_field(&m_field), new_item_string("$.x"));
  result = pq_not_support_func(item5);
  EXPECT_EQ(true, result);

  auto item6 = new Item_func_release_lock(POS(), nullptr);
  result = pq_not_support_func(item6);
  EXPECT_EQ(true, result);
}

TEST_F(PqConditionTest, pq_not_support_aggr_functype) {
  bool result;

  result = pq_not_support_aggr_functype(Item_sum::COUNT_DISTINCT_FUNC);
  EXPECT_EQ(true, result);

  result = pq_not_support_aggr_functype(Item_sum::COUNT_FUNC);
  EXPECT_EQ(false, result);
}

TEST_F(PqConditionTest, pq_not_support_ref) {
  bool result;
  Item_ref *item = new Item_ref(POS(), "db", "table", "field");

  result = pq_not_support_ref(item);
  EXPECT_EQ(false, result);

  auto item_field = new Item_field(&m_field);
  item = new Item_outer_ref(nullptr, item_field, nullptr);
  result = pq_not_support_ref(item);
  EXPECT_EQ(true, result);
}

TEST_F(PqConditionTest, check_pq_support_fieldtype_of_field_item) {
  bool result;
  Item *item = new Item_field(&m_field);

  result = check_pq_support_fieldtype_of_field_item(item);
  EXPECT_EQ(false, result);
}

TEST_F(PqConditionTest, check_pq_support_fieldtype_of_func_item) {
  bool result;
  Item *item = new Item_func_release_all_locks(POS());

  result = check_pq_support_fieldtype_of_func_item(item);
  EXPECT_EQ(false, result);
}

TEST_F(PqConditionTest, check_pq_support_fieldtype_of_ref_item) {
  bool result;
  auto item_field = new Item_field(&m_field);
  Item *item = new Item_outer_ref(nullptr, item_field, nullptr);

  result = check_pq_support_fieldtype_of_ref_item(item);
  EXPECT_EQ(false, result);
}

TEST_F(PqConditionTest, check_pq_support_fieldtype_of_cache_item) {
  bool result;

  result = check_pq_support_fieldtype_of_cache_item(nullptr);
  EXPECT_EQ(false, result);
}

TEST_F(PqConditionTest, check_pq_support_fieldtype) {
  bool result;
  Item *item = new Item_field(&m_field);

  result = check_pq_support_fieldtype(item);
  EXPECT_EQ(false, result);
}

TEST_F(PqConditionTest, choose_parallel_scan_table) {
  bool result;
  SELECT_LEX *select_lex = parse(&initializer, "SELECT * FROM t1", 0);
  JOIN *join = new (&m_mem_root) JOIN(initializer.thd(), select_lex);
  join->tables = 1;
  join->qep_tab = new (&m_mem_root) QEP_TAB();
  join->qep_tab->set_qs(new (&m_mem_root) QEP_shared);

  result = choose_parallel_scan_table(join);
  EXPECT_EQ(true, result);
}

TEST_F(PqConditionTest, set_pq_dop) {
  THD *thd = initializer.thd();
  thd->no_pq = true;
  thd->pq_dop = 2;
  thd->variables.parallel_default_dop = 4;

  set_pq_dop(thd);
  EXPECT_NE(thd->pq_dop, thd->variables.parallel_default_dop);

  thd->no_pq = false;
  thd->variables.force_parallel_execute = false;
  set_pq_dop(thd);
  EXPECT_NE(thd->pq_dop, thd->variables.parallel_default_dop);

  thd->variables.force_parallel_execute = true;
  set_pq_dop(thd);
  EXPECT_NE(thd->pq_dop, thd->variables.parallel_default_dop);

  thd->pq_dop = 0;
  set_pq_dop(thd);
  EXPECT_EQ(thd->pq_dop, thd->variables.parallel_default_dop);
}

TEST_F(PqConditionTest, set_pq_condition_status) {
  THD *thd = initializer.thd();
  thd->no_pq = false;
  thd->pq_dop = 0;
  thd->variables.force_parallel_execute = true;
  thd->variables.parallel_default_dop = 4;

  set_pq_condition_status(thd);
  EXPECT_EQ(PqConditionStatus::ENABLED, thd->m_suite_for_pq);

  thd->no_pq = true;
  thd->pq_dop = 0;
  set_pq_condition_status(thd);
  EXPECT_EQ(PqConditionStatus::NOT_SUPPORTED, thd->m_suite_for_pq);
}

TEST_F(PqConditionTest, suite_for_parallel_query_thd) {
  THD *thd = initializer.thd();
  thd->in_sp_trigger = false;
  thd->m_attachable_trx = nullptr;
  thd->tx_isolation = ISO_READ_UNCOMMITTED;
  bool result;

  result = suite_for_parallel_query(thd);
  EXPECT_EQ(true, result);

  thd->tx_isolation = ISO_SERIALIZABLE;
  result = suite_for_parallel_query(thd);
  EXPECT_EQ(false, result);

  thd->tx_isolation = ISO_READ_UNCOMMITTED;
  thd->begin_attachable_ro_transaction();
  result = suite_for_parallel_query(thd);
  EXPECT_EQ(false, result);
  thd->end_attachable_transaction();

  thd->in_sp_trigger = true;
  result = suite_for_parallel_query(thd);
  EXPECT_EQ(false, result);
}

TEST_F(PqConditionTest, suite_for_parallel_query_join) {
  bool result;
  SELECT_LEX *select_lex = parse(&initializer, "SELECT * FROM t1", 0);
  JOIN *join = new (&m_mem_root) JOIN(initializer.thd(), select_lex);

  join->best_read = 0;
  join->primary_tables = 1;
  join->saved_tmp_table_param = new (&m_mem_root) Temp_table_param();
  result = suite_for_parallel_query(join);
  EXPECT_EQ(false, result);

  join->best_read = 1000;
  join->primary_tables = 0;
  result = suite_for_parallel_query(join);
  EXPECT_EQ(false, result);

  join->primary_tables = 1;
  join->select_distinct = true;
  result = suite_for_parallel_query(join);
  EXPECT_EQ(false, result);
}

TEST_F(PqConditionTest, check_pq_running_threads) {
  uint dop = 1;
  ulong timeout_ms = 0;
  bool result;

  parallel_threads_running = 0;
  parallel_max_threads = 2;

  result = check_pq_running_threads(dop, timeout_ms);
  EXPECT_EQ(true, result);
  EXPECT_EQ(1, current_thd->pq_threads_running);
  EXPECT_EQ(1, parallel_threads_running);

  result = check_pq_running_threads(dop, timeout_ms);
  EXPECT_EQ(true, result);
  EXPECT_EQ(2, current_thd->pq_threads_running);
  EXPECT_EQ(2, parallel_threads_running);

  result = check_pq_running_threads(dop, timeout_ms);
  EXPECT_EQ(false, result);
  
  timeout_ms = 1;
  result = check_pq_running_threads(dop, timeout_ms);
  EXPECT_EQ(false, result);
}

TEST_F(PqConditionTest, check_pq_conditions) {
  bool result;
  THD *thd = initializer.thd();
  SELECT_LEX *select = thd->lex->unit->first_select();
  JOIN *join = new (&m_mem_root) JOIN(thd, select);
  select->join = join;

  result = check_pq_conditions(thd);
  EXPECT_EQ(false, result);
}

} // namespace parallel_query_test


