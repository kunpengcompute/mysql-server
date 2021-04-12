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

#ifndef lock0aarch64_atomic_h
#define lock0aarch64_atomic_h

#include "univ.i"

lint word_add_fetch(volatile lint *word, ulint amount);

#endif /* lock0aarch64_atomic_h */