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

#include "sql/sql_parallel.h"
#include "sql/exchange.h"


#include "unittest/gunit/test_utils.h"
#include "unittest/gunit/parsertest.h"
#include "unittest/gunit/base_mock_field.h"
#include "unittest/gunit/fake_table.h"

using namespace std;
using my_testing::Server_initializer;

namespace MessageQueue_unittest {

class MessageQueueTest :  public ::testing::Test {
protected:
  virtual void SetUp() { 
      initializer.SetUp();
      thd = initializer.thd();
    }
  virtual void TearDown() { initializer.TearDown(); }

  Server_initializer initializer;
  THD *thd;
};


TEST_F(MessageQueueTest, InitMqueueHandle) {
  MQueue_handle handleTest;
  bool ret;
  ret = handleTest.init_mqueue_handle(thd);
  EXPECT_EQ(true, ret);

  MQueue mq;
  int bufferLen = 1024;
  MQueue_handle handleTest2(&mq,bufferLen);
  ret = handleTest2.init_mqueue_handle(thd);
  EXPECT_EQ(false, ret);

}

// test the normal branch
TEST_F(MessageQueueTest, SendMsg) {
  MQueue mq;
  int bufferLen = 10;
  char buffer[10];
  mq.m_buffer = buffer;
  mq.m_ring_size = bufferLen;
  MQ_event send_event, receiver_event;
  mq.m_sender_event = &send_event;
  mq.m_receiver_event = &receiver_event;
  MQueue_handle mqHandle(&mq,bufferLen);
  mqHandle.init_mqueue_handle(thd);

  char data[5] = "aaaa";
  MQ_RESULT ret;

  ret = mqHandle.send((void*)data, 5, false);
  EXPECT_EQ(MQ_SUCCESS, ret);
}

// test the exception branch
TEST_F(MessageQueueTest, SendMsgError) {
  MQueue mq;
  int bufferLen = 10;
  char buffer[10];
  mq.m_buffer = buffer;
  mq.m_ring_size = bufferLen;
  MQ_event send_event, receiver_event;
  mq.m_sender_event = &send_event;
  mq.m_receiver_event = &receiver_event;
  MQueue_handle mqHandle(&mq,bufferLen);
  mqHandle.init_mqueue_handle(thd);

  char data[5] = "aaaa";
  MQ_RESULT ret;
  //pq error test
  thd->pq_error = true;
  ret = mqHandle.send((void*)data, 5, false);
  EXPECT_EQ(MQ_DETACHED, ret);
  thd->pq_error = false; // default value

  // MQ DETACHED test
  mqHandle.m_queue->detached = MQ_HAVE_DETACHED;
  ret = mqHandle.send((void*)data, 5, false);
  EXPECT_EQ(MQ_DETACHED, ret);

  mqHandle.m_queue->detached = MQ_TMP_DETACHED;
  ret = mqHandle.send((void*)data, 5, false);
  EXPECT_EQ(MQ_SUCCESS, ret);

  //available == 0 test 
  MQueue mq2;
  char buffer2[10];
  mq2.m_buffer = buffer2;
  mq2.m_ring_size = 0;
  MQ_event send_event2, receiver_event2;
  mq2.m_sender_event = &send_event2;
  mq2.m_receiver_event = &receiver_event2;

  MQueue_handle mqHandle2(&mq2,bufferLen);
  mqHandle2.init_mqueue_handle(thd);

  ret = mqHandle2.send((void*)data, 5, true);
  EXPECT_EQ(MQ_WOULD_BLOCK, ret);
} 

// test the raw data msg
TEST_F(MessageQueueTest, SendRawDataMsg) {
  MQueue mq;
  int bufferLen = 10;
  char buffer[10];
  MQ_RESULT ret;
  MQ_event send_event, receiver_event;
  mq.m_buffer = buffer;
  mq.m_ring_size = bufferLen;
  mq.m_sender_event = &send_event;
  mq.m_receiver_event = &receiver_event;
  MQueue_handle mqHandle(&mq,bufferLen);
  mqHandle.init_mqueue_handle(thd);

  Field_raw_data rawData;
  uchar data[5] = "aaaa";
  rawData.m_len = 5;
  rawData.m_ptr = data;

  ret = mqHandle.send(&rawData);
  EXPECT_EQ(MQ_SUCCESS, ret);

}


} // end namespace parallelscaniterator_unittest
