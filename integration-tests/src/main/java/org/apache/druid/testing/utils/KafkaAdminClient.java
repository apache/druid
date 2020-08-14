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

package org.apache.druid.testing.utils;

import com.google.common.collect.ImmutableList;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaAdminClient implements StreamAdminClient
{
  private AdminClient adminClient;

  public KafkaAdminClient(IntegrationTestingConfig config)
  {
    Properties properties = new Properties();
    KafkaUtil.addPropertiesFromTestConfig(config, properties);
    properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaHost());
    adminClient = AdminClient.create(properties);
  }

  @Override
  public void createStream(String streamName, int partitionCount, Map<String, String> tags) throws Exception
  {
    final short replicationFactor = 1;
    final NewTopic newTopic = new NewTopic(streamName, partitionCount, replicationFactor);
    final CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(newTopic));
    // Wait for create topic to compelte
    createTopicsResult.values().get(streamName).get();
  }

  @Override
  public void deleteStream(String streamName) throws Exception
  {
    DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(ImmutableList.of(streamName));
    deleteTopicsResult.values().get(streamName).get();
  }

  /**
   * This method can only increase the partition count of {@param streamName} to have a final partition
   * count of {@param newPartitionCount}
   * If {@param blocksUntilStarted} is set to true, then this method will blocks until the partitioning
   * started (but not nessesary finished), otherwise, the method will returns right after issue the
   * repartitioning command
   */
  @Override
  public void updatePartitionCount(String streamName, int newPartitionCount, boolean blocksUntilStarted) throws Exception
  {
    Map<String, NewPartitions> counts = new HashMap<>();
    counts.put(streamName, NewPartitions.increaseTo(newPartitionCount));
    CreatePartitionsResult createPartitionsResult = adminClient.createPartitions(counts);
    if (blocksUntilStarted) {
      createPartitionsResult.values().get(streamName).get();

    }
  }

  /**
   * Stream state such as active/non-active does not applies to Kafka.
   * Returning true since Kafka stream is always active and can always be writen and read to.
   */
  @Override
  public boolean isStreamActive(String streamName)
  {
    return true;
  }

  @Override
  public int getStreamPartitionCount(String streamName) throws Exception
  {
    DescribeTopicsResult result = adminClient.describeTopics(ImmutableList.of(streamName));
    TopicDescription topicDescription = result.values().get(streamName).get();
    return topicDescription.partitions().size();
  }

  @Override
  public boolean verfiyPartitionCountUpdated(String streamName, int oldPartitionCount, int newPartitionCount) throws Exception
  {
    return getStreamPartitionCount(streamName) == newPartitionCount;
  }
}
