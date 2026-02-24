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

package org.apache.druid.testing.embedded.kinesis;

import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.StreamIngestResource;
import org.apache.druid.testing.embedded.indexing.StreamIndexFaultToleranceTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class KinesisFaultToleranceTest extends StreamIndexFaultToleranceTest
{
  private final AtomicBoolean publishToSingleShard = new AtomicBoolean(false);
  private final KinesisResource kinesis = new KinesisResource()
  {
    @Override
    public void publishRecordsToTopicWithoutTransaction(String topic, List<byte[]> records)
    {
      if (publishToSingleShard.get()) {
        super.publishRecordsToTopicWithoutTransaction(topic, records);
      } else {
        super.publishRecordsToTopicPartition(topic, "O", records);
      }
    }
  };

  @BeforeEach
  public void resetState()
  {
    publishToSingleShard.set(false);
  }

  @Override
  protected StreamIngestResource<?> getStreamIngestResource()
  {
    return kinesis;
  }

  @Override
  protected SupervisorSpec createSupervisorSpec(String dataSource, String topic)
  {
    return createKinesisSupervisor(kinesis, dataSource, topic);
  }

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return super.createCluster().useDefaultTimeoutForLatchableEmitter(120);
  }

  @Override
  @ParameterizedTest(name = "useTransactions={0}")
  @ValueSource(booleans = {false})
  public void test_supervisorRecovers_afterOverlordRestart(boolean useTransactions) throws Exception
  {
    super.test_supervisorRecovers_afterOverlordRestart(useTransactions);
  }

  @Override
  @ParameterizedTest(name = "useTransactions={0}")
  @ValueSource(booleans = {false})
  public void test_supervisorRecovers_afterSuspendResume(boolean useTransactions)
  {
    super.test_supervisorRecovers_afterSuspendResume(useTransactions);
  }

  @Override
  @ParameterizedTest(name = "useTransactions={0}")
  @ValueSource(booleans = {false})
  public void test_supervisorRecovers_afterChangeInTopicPartitions(boolean useTransactions)
  {
    super.test_supervisorRecovers_afterChangeInTopicPartitions(useTransactions);
  }

  @Test
  public void test_supervisorRecovers_afterSuspendResume_withEmptyShards()
  {
    publishToSingleShard.set(true);
    super.test_supervisorRecovers_afterSuspendResume(false);
  }

  @Test
  public void test_supervisorRecovers_afterChangeInShardCount_withEmptyShards()
  {
    publishToSingleShard.set(true);
    super.test_supervisorRecovers_afterChangeInTopicPartitions(false);
  }
}
