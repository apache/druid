/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.rocketmq.test;

import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class TestProducer
{

  private static int confirmRetry = 20;

  public static void produceAndConfirm(TestBroker testBroker, List<Pair<MessageQueue, Message>> records) throws MQClientException, RemotingException, InterruptedException, MQBrokerException
  {
    DefaultMQProducer producer = testBroker.newProducer();
    DefaultLitePullConsumer consumer = new DefaultLitePullConsumer("test_consumer" + ThreadLocalRandom.current().nextInt(99999));
    consumer.setNamesrvAddr(StringUtils.format("127.0.0.1:%d", testBroker.getPort()));
    consumer.start();
    MQClientInstance clientInstance = MQClientManager.getInstance().getOrCreateMQClientInstance(consumer);

    HashMap<MessageQueue, Long> messageQueueHashMap = new HashMap<>();
    for (Pair<MessageQueue, Message> record : records) {
      if (messageQueueHashMap.containsKey(record.lhs)) {
        messageQueueHashMap.put(record.lhs, messageQueueHashMap.get(record.lhs) + 1L);
      } else {
        messageQueueHashMap.put(record.lhs, 1L);
      }
    }
    for (MessageQueue mq : messageQueueHashMap.keySet()) {
      long offset = 0L;
      try {
        offset = clientInstance.getMQAdminImpl().maxOffset(mq);
      }
      catch (MQClientException e) {
        e.printStackTrace();
      }
      messageQueueHashMap.put(mq, messageQueueHashMap.get(mq) + offset);
    }

    producer.start();
    for (Pair<MessageQueue, Message> record : records) {
      producer.send(record.rhs, record.lhs).getQueueOffset();
    }
    producer.shutdown();

    int confirmCount = 0;
    for (int i = 0; i < confirmRetry && confirmCount < messageQueueHashMap.size(); i++) {
      for (MessageQueue mq : messageQueueHashMap.keySet()) {
        if (clientInstance.getMQAdminImpl().maxOffset(mq) == messageQueueHashMap.get(mq)) {
          confirmCount++;
        }
      }
      Thread.sleep(500);
    }
  }
}
