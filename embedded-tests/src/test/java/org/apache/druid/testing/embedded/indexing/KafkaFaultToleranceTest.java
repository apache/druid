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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class KafkaFaultToleranceTest extends KafkaTestBase
{
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void test_supervisorRecovers_afterOverlordRestart(boolean useTransactions) throws Exception
  {
    setupTopicAndSupervisor(2, useTransactions);
    overlord.stop();
    publishRecords(topic, useTransactions);
    overlord.start();
    verifyDataAndTearDown(useTransactions);
  }

  @Test
  public void test_supervisorRecovers_afterCoordinatorRestart() throws Exception
  {
    final boolean useTransactions = true;
    setupTopicAndSupervisor(3, useTransactions);
    coordinator.stop();
    publishRecords(topic, useTransactions);
    coordinator.start();
    verifyDataAndTearDown(useTransactions);
  }

  @Test
  public void test_supervisorRecovers_afterHistoricalRestart() throws Exception
  {
    final boolean useTransactions = false;
    setupTopicAndSupervisor(2, useTransactions);
    historical.stop();
    publishRecords(topic, useTransactions);
    historical.start();
    verifyDataAndTearDown(useTransactions);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void test_supervisorRecovers_afterSuspendResume(boolean useTransactions)
  {
    setupTopicAndSupervisor(2, useTransactions);
    cluster.callApi().postSupervisor(supervisorSpec.createSuspendedSpec());
    publishRecords(topic, useTransactions);
    cluster.callApi().postSupervisor(supervisorSpec.createRunningSpec());
    verifyDataAndTearDown(useTransactions);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void test_supervisorRecovers_afterChangeInTopicPartitions(boolean useTransactions)
  {
    setupTopicAndSupervisor(2, useTransactions);
    kafkaServer.increasePartitionsInTopic(topic, 4);
    publishRecords(topic, useTransactions);
    verifyDataAndTearDown(useTransactions);
  }
}
