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

package org.apache.druid.testing.embedded.indexing;

import org.apache.druid.indexing.kafka.simulate.KafkaResource;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.testcontainers.kafka.KafkaContainer;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * A {@link KafkaResource} that starts a Kafka broker with share groups enabled.
 * Share groups (KIP-932) require the broker config {@code group.share.enable=true}.
 */
public class ShareGroupKafkaResource extends KafkaResource
{
  @Override
  protected KafkaContainer createContainer()
  {
    final KafkaContainer container = super.createContainer();
    container.withEnv("KAFKA_GROUP_SHARE_ENABLE", "true");
    container.withEnv("KAFKA_GROUP_SHARE_RECORD_LOCK_DURATION_MS", "30000");
    // Single-broker test cluster: override replication for the internal
    // __share_group_state topic so the share coordinator can initialize.
    container.withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR", "1");
    container.withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR", "1");
    return container;
  }

  /**
   * Sets {@code share.auto.offset.reset} for the named share group via
   * AdminClient. Broker default is {@code LATEST}; tests that produce
   * records before subscribing need {@code earliest}.
   */
  public void setShareGroupAutoOffsetReset(String groupId, String value)
  {
    try (Admin admin = newAdminClient()) {
      final ConfigResource resource = new ConfigResource(ConfigResource.Type.GROUP, groupId);
      final Collection<AlterConfigOp> ops = List.of(
          new AlterConfigOp(
              new ConfigEntry("share.auto.offset.reset", value),
              AlterConfigOp.OpType.SET
          )
      );
      admin.incrementalAlterConfigs(Map.of(resource, ops)).all().get();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
