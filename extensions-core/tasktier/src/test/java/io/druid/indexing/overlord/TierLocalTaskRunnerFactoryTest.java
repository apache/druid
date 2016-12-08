/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.indexing.overlord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.http.client.HttpClient;
import io.druid.curator.discovery.ServiceAnnouncer;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.indexing.overlord.config.TierLocalTaskRunnerConfig;
import io.druid.indexing.worker.config.WorkerConfig;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.server.DruidNode;
import io.druid.tasklogs.TaskLogPusher;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

public class TierLocalTaskRunnerFactoryTest
{

  final TierLocalTaskRunnerConfig config = EasyMock.createStrictMock(TierLocalTaskRunnerConfig.class);
  final TaskConfig taskConfig = EasyMock.createStrictMock(TaskConfig.class);
  final WorkerConfig workerConfig = EasyMock.createStrictMock(WorkerConfig.class);
  final Properties props = new Properties();
  final TaskLogPusher taskLogPusher = EasyMock.createStrictMock(TaskLogPusher.class);
  final ObjectMapper jsonMapper = new DefaultObjectMapper();
  final DruidNode node = new DruidNode("some:service", "localhost", -1);
  final HttpClient httpClient = EasyMock.createStrictMock(HttpClient.class);
  final ServiceAnnouncer serviceAnnouncer = EasyMock.createStrictMock(ServiceAnnouncer.class);

  @Before
  public void setUp()
  {
    EasyMock.replay(config, taskConfig, workerConfig, taskLogPusher, httpClient, serviceAnnouncer);
  }

  @Test
  public void testBuild() throws Exception
  {
    EasyMock.reset(workerConfig, config);
    EasyMock.expect(workerConfig.getCapacity()).andReturn(1).times(2);
    EasyMock.expect(config.getStartPort()).andReturn(9999).once();
    EasyMock.expect(config.getHeartbeatLocalNetworkTimeout()).andReturn(9999L).once();
    EasyMock.expect(config.getDelayBetweenHeartbeatBatches()).andReturn(1L).once();
    EasyMock.replay(workerConfig, config);
    final TierLocalTaskRunnerFactory factory = new TierLocalTaskRunnerFactory(
        config,
        taskConfig,
        workerConfig,
        props,
        taskLogPusher,
        jsonMapper,
        node,
        httpClient,
        serviceAnnouncer
    );
    final TaskRunner runner = factory.build();
    Assert.assertTrue(runner instanceof TierLocalTaskRunner);
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(config, taskConfig, workerConfig, taskLogPusher, httpClient, serviceAnnouncer);
  }
}
