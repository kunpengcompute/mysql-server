#ifndef PQ_CLONE_INCLUDE_H
#define PQ_CLONE_INCLUDE_H

/* Copyright (c) 2020, Huawei and/or its affiliates. All rights reserved.

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

#include "sql/sql_list.h"
class Item;
class Item_ident;
class THD;
class SELECT_LEX;
class JOIN;
class ORDER;
class ORDER_with_src;
class TABLE_LIST;
enum table_list_type_enum {
  TABLE_LIST_TYPE_DEFAULT,
  TABLE_LIST_TYPE_LEAF,
  TABLE_LIST_TYPE_GLOBAL,
  TABLE_LIST_TYPE_MERGE
};

bool pq_dup_tabs(JOIN *pq_join, JOIN *join, bool setup);

TABLE_LIST *get_table_by_index(TABLE_LIST* start_table, table_list_type_enum list_type, int index);
int get_table_index(TABLE_LIST* start_table, table_list_type_enum list_type, TABLE_LIST *tl);
extern Item **resolve_ref_in_select_and_group(THD *thd, Item_ident *ref,
                                              SELECT_LEX *select);

JOIN *pq_make_join(THD *thd, JOIN *join);
#endif  // PQ_CLONE_INCLUDE_H
