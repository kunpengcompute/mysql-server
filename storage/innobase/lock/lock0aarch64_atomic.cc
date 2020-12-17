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

#include "lock0aarch64_atomic.h"

lint word_add_fetch(volatile lint *word, ulint amount) {
  asm volatile (
    "ldaddal %0, x3, [%1]\n\t"
    "add %0, x3, %0"
      :"+r"(amount)
      :"r"(word)
      :"x3","memory"
  );
  return amount;
}