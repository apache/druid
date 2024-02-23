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

package org.apache.druid.data.input.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class KafkaTopicPartitionTest
{
  private ObjectMapper objectMapper;

  @Before
  public void setUp()
  {
    objectMapper = new ObjectMapper();
    objectMapper.registerSubtypes(KafkaTopicPartition.class);
    SimpleModule simpleModule = new SimpleModule();
    simpleModule.addKeySerializer(KafkaTopicPartition.class,
                                  new KafkaTopicPartition.KafkaTopicPartitionKeySerializer());
    objectMapper.registerModule(simpleModule);
  }

  @Test
  public void testEquals()
  {
    KafkaTopicPartition partition1 = new KafkaTopicPartition(false, "topic", 0);
    KafkaTopicPartition partition2 = new KafkaTopicPartition(false, "topic", 0);
    KafkaTopicPartition partition3 = new KafkaTopicPartition(false, "topic", 1);
    KafkaTopicPartition partition4 = new KafkaTopicPartition(false, "topic2", 0);
    KafkaTopicPartition partition5 = new KafkaTopicPartition(false, null, 0);
    KafkaTopicPartition partition6 = new KafkaTopicPartition(false, null, 0);
    KafkaTopicPartition partition7 = new KafkaTopicPartition(true, "topic", 0);
    KafkaTopicPartition partition8 = new KafkaTopicPartition(true, "topic2", 0);

    Assert.assertEquals(partition1, partition2);
    Assert.assertNotEquals(partition1, partition3);
    Assert.assertEquals(partition1, partition4);
    Assert.assertEquals(partition5, partition6);
    Assert.assertEquals(partition1, partition5);
    Assert.assertNotEquals(partition1, partition7);
    Assert.assertNotEquals(partition7, partition8);
  }

  @Test
  public void testHashCode()
  {
    KafkaTopicPartition partition1 = new KafkaTopicPartition(false, "topic", 0);
    KafkaTopicPartition partition2 = new KafkaTopicPartition(false, "topic", 0);
    KafkaTopicPartition partition3 = new KafkaTopicPartition(false, "topic", 1);
    KafkaTopicPartition partition4 = new KafkaTopicPartition(false, "topic2", 0);
    KafkaTopicPartition partition5 = new KafkaTopicPartition(false, null, 0);
    KafkaTopicPartition partition6 = new KafkaTopicPartition(false, null, 0);
    KafkaTopicPartition partition7 = new KafkaTopicPartition(true, "topic", 0);
    KafkaTopicPartition partition8 = new KafkaTopicPartition(true, "topic2", 0);

    Assert.assertEquals(partition1.hashCode(), partition2.hashCode());
    Assert.assertNotEquals(partition1.hashCode(), partition3.hashCode());
    Assert.assertEquals(partition1.hashCode(), partition4.hashCode());
    Assert.assertEquals(partition5.hashCode(), partition6.hashCode());
    Assert.assertEquals(partition1.hashCode(), partition5.hashCode());
    Assert.assertNotEquals(partition1.hashCode(), partition7.hashCode());
    Assert.assertNotEquals(partition7.hashCode(), partition8.hashCode());
  }

  @Test
  public void testMultiTopicDeserialization() throws JsonProcessingException
  {
    KafkaTopicPartition partition = objectMapper.readerFor(KafkaTopicPartition.class).readValue("\"topic:0\"");
    Assert.assertEquals(0, partition.partition());
    Assert.assertEquals("topic", partition.topic().orElse(null));
    Assert.assertTrue(partition.isMultiTopicPartition());
  }

  @Test
  public void testSingleTopicDeserialization() throws JsonProcessingException
  {
    KafkaTopicPartition partition = objectMapper.readerFor(KafkaTopicPartition.class).readValue("0");
    Assert.assertEquals(0, partition.partition());
    Assert.assertNull(partition.topic().orElse(null));
    Assert.assertFalse(partition.isMultiTopicPartition());
  }

  @Test
  public void testMultiTopicSerialization() throws JsonProcessingException
  {
    KafkaTopicPartition partition = new KafkaTopicPartition(true, "topic", 0);
    KafkaTopicPartition reincarnated = objectMapper.readerFor(KafkaTopicPartition.class).readValue(objectMapper.writeValueAsString(partition));
    Assert.assertEquals(partition, reincarnated);
  }

  @Test
  public void testSingleTopicSerialization() throws JsonProcessingException
  {
    KafkaTopicPartition partition = new KafkaTopicPartition(false, null, 0);
    KafkaTopicPartition reincarnated = objectMapper.readerFor(KafkaTopicPartition.class).readValue(objectMapper.writeValueAsString(partition));
    Assert.assertEquals(partition, reincarnated);
  }
}
