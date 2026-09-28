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

package org.apache.druid.indexing.kafka;

import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.ISE;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KafkaShareGroupSourceIdentityResolverTest
{
  @Test
  public void testResolveUsesBrokerClusterAndTopicIds()
  {
    final Uuid topicId = Uuid.randomUuid();
    final Admin admin = admin("cluster-id", new TopicDescription("topic", false, List.of(), Set.of(), topicId));

    final ShareGroupSourceIdentity identity = KafkaShareGroupSourceIdentityResolver.resolve(
        admin,
        "topic",
        Duration.ofSeconds(1)
    );

    Assertions.assertEquals("cluster-id", identity.getClusterId());
    Assertions.assertEquals(topicId.toString(), identity.getTopicId());
    Assertions.assertEquals("topic", identity.getTopicName());
  }

  @Test
  public void testResolveRejectsMissingStableTopicId()
  {
    final Admin admin = admin(
        "cluster-id",
        new TopicDescription("topic", false, List.of(), Set.of(), Uuid.ZERO_UUID)
    );

    Assertions.assertThrows(
        ISE.class,
        () -> KafkaShareGroupSourceIdentityResolver.resolve(admin, "topic", Duration.ofSeconds(1))
    );
  }

  @Test
  public void testAdminPropertiesExcludeConsumerOnlyConfiguration()
  {
    final Properties properties = KafkaShareGroupSourceIdentityResolver.adminProperties(
        Map.of(
            "bootstrap.servers", "localhost:9092",
            "auto.offset.reset", "earliest",
            "group.id", "share-group"
        ),
        new DefaultObjectMapper()
    );

    Assertions.assertEquals("localhost:9092", properties.getProperty("bootstrap.servers"));
    Assertions.assertNull(properties.getProperty("auto.offset.reset"));
    Assertions.assertNull(properties.getProperty("group.id"));
  }

  private static Admin admin(String clusterId, TopicDescription topicDescription)
  {
    final Admin admin = mock(Admin.class);
    final DescribeClusterResult clusterResult = mock(DescribeClusterResult.class);
    final DescribeTopicsResult topicsResult = mock(DescribeTopicsResult.class);
    when(admin.describeCluster()).thenReturn(clusterResult);
    when(clusterResult.clusterId()).thenReturn(KafkaFuture.completedFuture(clusterId));
    when(admin.describeTopics(List.of("topic"))).thenReturn(topicsResult);
    when(topicsResult.topicNameValues()).thenReturn(
        Map.of("topic", KafkaFuture.completedFuture(topicDescription))
    );
    return admin;
  }
}
