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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.ISE;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Uuid;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class KafkaShareGroupSourceIdentityResolver
{
  private KafkaShareGroupSourceIdentityResolver()
  {
  }

  public static ShareGroupSourceIdentity resolve(Admin admin, String topic, Duration timeout)
  {
    try {
      final String clusterId = admin.describeCluster().clusterId().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
      final Map<String, KafkaFuture<TopicDescription>> descriptions = admin.describeTopics(List.of(topic))
                                                                            .topicNameValues();
      final KafkaFuture<TopicDescription> descriptionFuture = descriptions.get(topic);
      if (descriptionFuture == null) {
        throw new ISE("Kafka did not return metadata for topic[%s]", topic);
      }
      final TopicDescription description = descriptionFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
      if (clusterId == null || clusterId.isEmpty()) {
        throw new ISE("Kafka did not return a cluster ID for topic[%s]", topic);
      }
      if (description.topicId() == null || Uuid.ZERO_UUID.equals(description.topicId())) {
        throw new ISE("Kafka did not return a stable topic ID for topic[%s]", topic);
      }
      if (!topic.equals(description.name())) {
        throw new ISE("Kafka returned metadata for topic[%s] while resolving topic[%s]", description.name(), topic);
      }
      return new ShareGroupSourceIdentity(clusterId, description.topicId().toString(), description.name());
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ISE(e, "Interrupted while resolving Kafka identity for topic[%s]", topic);
    }
    catch (ExecutionException | TimeoutException e) {
      throw new ISE(e, "Unable to resolve Kafka identity for topic[%s]", topic);
    }
  }

  public static Admin createAdmin(Map<String, Object> consumerProperties, ObjectMapper configMapper)
  {
    final ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(KafkaShareGroupSourceIdentityResolver.class.getClassLoader());
      return Admin.create(adminProperties(consumerProperties, configMapper));
    }
    finally {
      Thread.currentThread().setContextClassLoader(currentClassLoader);
    }
  }

  static Properties adminProperties(Map<String, Object> consumerProperties, ObjectMapper configMapper)
  {
    final Properties resolved = new Properties();
    KafkaRecordSupplier.addConsumerPropertiesFromConfig(resolved, configMapper, consumerProperties);
    final Properties adminProperties = new Properties();
    for (String key : resolved.stringPropertyNames()) {
      if (AdminClientConfig.configNames().contains(key)) {
        adminProperties.setProperty(key, resolved.getProperty(key));
      }
    }
    return adminProperties;
  }
}
