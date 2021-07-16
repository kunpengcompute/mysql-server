/*Copyright (c) 2001, 2021, Oracle and/or its affiliates. All rights reserved. 
   Copyright (c) 2021, Huawei Technologies Co., Ltd. 
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
#include "unittest/gunit/test_utils.h"
#include "unittest/gunit/parsertest.h"
#include "unittest/gunit/base_mock_field.h"
#include "unittest/gunit/fake_table.h"
#include "sql/pq_clone.h"
using namespace std;
using my_testing::Server_initializer;

namespace CopyTablelist_unittest {

class CopyTablelistTest :  public ::testing::Test {
protected:
  virtual void SetUp() { 
    currentInitializer.SetUp();
    currentThd = currentInitializer.thd();
    origInitializer.SetUp();
    origThd = origInitializer.thd();
  }
  virtual void TearDown() {
    currentInitializer.TearDown();
    origInitializer.TearDown();
  }

  Server_initializer currentInitializer;
  Server_initializer origInitializer;
  THD *currentThd;
  THD *origThd;
  
  TABLE_LIST* createTableList(THD *thd, const char* name, size_t table_name_length) {
    TABLE_LIST *ptr = new (thd->mem_root) TABLE_LIST;
    if (ptr == nullptr) {
      return nullptr;
    }
    ptr->select_lex = nullptr;
    ptr->derived = nullptr;
    ptr->effective_algorithm = VIEW_ALGORITHM_UNDEFINED;
    ptr->outer_join = JOIN_TYPE_INNER;
    ptr->field_translation = nullptr;
    ptr->table_name = name;
    ptr->table_name_length = table_name_length;
    ptr->alias = name;
    ptr->is_alias = false;
    ptr->table_function = nullptr;
    ptr->is_fqtn = false;
    ptr->db = "test";
    ptr->db_length = 4;
    ptr->set_tableno(0);
    ptr->set_lock({TL_UNLOCK, THR_DEFAULT});
    ptr->updating = false;
    ptr->force_index = false;
    ptr->ignore_leaves = false;
    ptr->is_system_view = false;
    ptr->cacheable_table = true;
    ptr->index_hints = nullptr;
    ptr->option = nullptr;
    ptr->next_name_resolution_table = nullptr;
    ptr->partition_names = nullptr;
    MDL_REQUEST_INIT(&ptr->mdl_request, MDL_key::TABLE, ptr->db,
                    ptr->table_name, MDL_SHARED_READ, MDL_TRANSACTION);
    return ptr;          
  }
};

TEST_F(CopyTablelistTest, CopyTablelist) {
  SELECT_LEX *orig = origThd->lex->new_query(nullptr);
  TABLE_LIST *t1 = createTableList(origThd, "t1", 2);
  TABLE_LIST *t2 = createTableList(origThd, "t1", 2);
  TABLE_LIST *t3 = createTableList(origThd, "t1", 2);
  orig->leaf_tables = t1;
  t1->next_leaf = t2;
  t2->next_leaf = nullptr;
  t1->next_global = t2;
  t2->next_global = nullptr;
  t1->next_local = t2;
  t2->next_local = nullptr;
  t1->merge_underlying_list = t3;
  orig->table_list.link_in_list(t1, &t1->next_local);
  orig->table_list.link_in_list(t2, &t2->next_local);

  int count = 2;
  Field_translator *transl = (Field_translator *)origThd->stmt_arena->alloc(
              count * sizeof(Field_translator));
  if (transl == nullptr) {
    return ;
  }
  for (int i = 0; i < count; i++) {
    transl[i].name = nullptr;
    transl[i].item = nullptr;
  }
  t1->field_translation = transl;
  t1->field_translation_end = transl + count;
 
  SELECT_LEX *select = currentThd->lex->new_query(nullptr);
  select->orig = orig;
  bool ret = copy_all_table_list(currentThd, orig, select);
  EXPECT_EQ(true, ret);
}

}