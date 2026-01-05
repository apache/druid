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

import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.rpc.RequestBuilder;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class KafkaFaultToleranceTest extends KafkaTestBase
{
  private SupervisorSpec supervisorSpec = null;
  private String topic = null;
  private int totalRecords = 0;

  @BeforeEach
  public void setupTopicAndSupervisor()
  {
    totalRecords = 0;
    topic = "topic_" + dataSource;
    kafkaServer.createTopicWithPartitions(topic, 2);

    supervisorSpec = createSupervisor().withId("supe_" + dataSource).build(dataSource, topic);
    cluster.callApi().postSupervisor(supervisorSpec);
  }
  
  @AfterEach
  public void verifyAndTearDown()
  {
    waitUntilPublishedRecordsAreIngested(totalRecords);
    verifySupervisorIsRunningHealthy(supervisorSpec.getId());
    cluster.callApi().postSupervisor(supervisorSpec.createSuspendedSpec());
    kafkaServer.deleteTopic(topic);
    verifyRowCount(totalRecords);
  }

  @ParameterizedTest(name = "useTransactions={0}")
  @ValueSource(booleans = {true, false})
  public void test_supervisorRecovers_afterOverlordRestart(boolean useTransactions) throws Exception
  {
    totalRecords = publish1kRecords(topic, useTransactions);
    waitUntilPublishedRecordsAreIngested(totalRecords);

    overlord.stop();
    totalRecords += publish1kRecords(topic, useTransactions);

    overlord.start();
    totalRecords += publish1kRecords(topic, useTransactions);
  }

  @Test
  public void test_supervisorRecovers_afterCoordinatorRestart() throws Exception
  {
    final boolean useTransactions = true;
    totalRecords = publish1kRecords(topic, useTransactions);
    waitUntilPublishedRecordsAreIngested(totalRecords);

    coordinator.stop();
    totalRecords += publish1kRecords(topic, useTransactions);

    coordinator.start();
    totalRecords += publish1kRecords(topic, useTransactions);
  }

  @Test
  public void test_supervisorRecovers_afterHistoricalRestart() throws Exception
  {
    final boolean useTransactions = false;
    totalRecords = publish1kRecords(topic, useTransactions);
    waitUntilPublishedRecordsAreIngested(totalRecords);

    historical.stop();
    totalRecords += publish1kRecords(topic, useTransactions);

    historical.start();
    totalRecords += publish1kRecords(topic, useTransactions);
  }

  @ParameterizedTest(name = "useTransactions={0}")
  @ValueSource(booleans = {true, false})
  public void test_supervisorRecovers_afterSuspendResume(boolean useTransactions)
  {
    totalRecords = publish1kRecords(topic, useTransactions);
    waitUntilPublishedRecordsAreIngested(totalRecords);

    cluster.callApi().postSupervisor(supervisorSpec.createSuspendedSpec());
    totalRecords += publish1kRecords(topic, useTransactions);

    cluster.callApi().postSupervisor(supervisorSpec.createRunningSpec());
    totalRecords += publish1kRecords(topic, useTransactions);
  }

  @ParameterizedTest(name = "useTransactions={0}")
  @ValueSource(booleans = {true, false})
  public void test_supervisorRecovers_afterChangeInTopicPartitions(boolean useTransactions)
  {
    totalRecords = publish1kRecords(topic, useTransactions);

    kafkaServer.increasePartitionsInTopic(topic, 4);
    totalRecords += publish1kRecords(topic, useTransactions);
  }

  @Test
  public void test_supervisorLaunchesNewTask_ifEarlyHandoff()
  {
    final boolean useTransactions = true;
    totalRecords = publish1kRecords(topic, useTransactions);

    waitUntilPublishedRecordsAreIngested(totalRecords);

    final Set<String> taskIdsBeforeHandoff = getRunningTaskIds(dataSource);
    Assertions.assertFalse(taskIdsBeforeHandoff.isEmpty());

    final String path = StringUtils.format(
        "/druid/indexer/v1/supervisor/%s/taskGroups/handoff",
        supervisorSpec.getId()
    );
    cluster.callApi().serviceClient().onLeaderOverlord(
        mapper -> new RequestBuilder(HttpMethod.POST, path)
            .jsonContent(mapper, Map.of("taskGroupIds", List.of(0, 1))),
        null
    );

    // Wait for the handoff notice to be processed
    overlord.latchableEmitter().waitForEvent(
        event -> event.hasMetricName("ingest/notices/time")
                      .hasDimension(DruidMetrics.SUPERVISOR_ID, supervisorSpec.getId())
                      .hasDimension("noticeType", "handoff_task_group_notice")
    );

    totalRecords += publish1kRecords(topic, useTransactions);
    waitUntilPublishedRecordsAreIngested(totalRecords);

    // Verify that the running task IDs have changed
    final Set<String> taskIdsAfterHandoff = getRunningTaskIds(dataSource);
    Assertions.assertFalse(taskIdsAfterHandoff.isEmpty());
    Assertions.assertFalse(taskIdsBeforeHandoff.stream().anyMatch(taskIdsAfterHandoff::contains));
  }

  private Set<String> getRunningTaskIds(String dataSource)
  {
    return cluster.callApi()
                  .getTasks(dataSource, "running")
                  .stream()
                  .map(TaskStatusPlus::getId)
                  .collect(Collectors.toSet());
  }
}
