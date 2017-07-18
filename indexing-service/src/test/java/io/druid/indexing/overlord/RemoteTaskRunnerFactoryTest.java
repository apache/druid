/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


package io.druid.indexing.overlord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.metamx.http.client.HttpClient;
import io.druid.curator.PotentiallyGzippedCompressionProvider;
import io.druid.indexing.common.TestUtils;
import io.druid.indexing.overlord.autoscaling.ProvisioningSchedulerConfig;
import io.druid.indexing.overlord.autoscaling.SimpleWorkerProvisioningConfig;
import io.druid.indexing.overlord.autoscaling.SimpleWorkerProvisioningStrategy;
import io.druid.indexing.overlord.config.RemoteTaskRunnerConfig;
import io.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import io.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import io.druid.java.util.common.concurrent.ScheduledExecutors;
import io.druid.server.initialization.IndexerZkConfig;
import io.druid.server.initialization.ZkPathsConfig;
import junit.framework.Assert;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingCluster;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class RemoteTaskRunnerFactoryTest
{
  private static final Joiner joiner = Joiner.on("/");
  private static final String basePath = "/test/druid";
  private TestingCluster testingCluster;
  private CuratorFramework cf;
  private ObjectMapper jsonMapper;


  @Before
  public void setUp() throws Exception
  {
    TestUtils testUtils = new TestUtils();
    jsonMapper = testUtils.getTestObjectMapper();

    testingCluster = new TestingCluster(1);
    testingCluster.start();

    cf = CuratorFrameworkFactory.builder()
                                .connectString(testingCluster.getConnectString())
                                .retryPolicy(new ExponentialBackoffRetry(1, 10))
                                .compressionProvider(new PotentiallyGzippedCompressionProvider(false))
                                .build();
    cf.start();
    cf.blockUntilConnected();
  }

  @After
  public void tearDown() throws Exception
  {
    cf.close();
    testingCluster.stop();
  }

  @Test
  public void testExecNotSharedBetweenRunners()
  {
    final AtomicInteger executorCount = new AtomicInteger(0);
    RemoteTaskRunnerConfig config = new RemoteTaskRunnerConfig();
    IndexerZkConfig indexerZkConfig = new IndexerZkConfig(
        new ZkPathsConfig()
        {
          @Override
          public String getBase()
          {
            return basePath;
          }
        }, null, null, null, null, null
    );

    HttpClient httpClient = EasyMock.createMock(HttpClient.class);
    Supplier<WorkerBehaviorConfig> workerBehaviorConfig = EasyMock.createMock(Supplier.class);
    ScheduledExecutorFactory executorFactory = new ScheduledExecutorFactory()
    {
      @Override
      public ScheduledExecutorService create(int i, String s)
      {
        executorCount.incrementAndGet();
        return ScheduledExecutors.fixed(i, s);
      }
    };
    SimpleWorkerProvisioningConfig provisioningConfig = new SimpleWorkerProvisioningConfig();
    ProvisioningSchedulerConfig provisioningSchedulerConfig = new ProvisioningSchedulerConfig()
    {
      @Override
      public boolean isDoAutoscale()
      {
        return true;
      }
    };
    RemoteTaskRunnerFactory factory = new RemoteTaskRunnerFactory(
        cf,
        config,
        indexerZkConfig,
        jsonMapper,
        httpClient,
        workerBehaviorConfig,
        executorFactory,
        provisioningSchedulerConfig,
        new SimpleWorkerProvisioningStrategy(
            provisioningConfig,
            workerBehaviorConfig,
            provisioningSchedulerConfig
        )
    );
    Assert.assertEquals(0, executorCount.get());
    RemoteTaskRunner remoteTaskRunner1 = factory.build();
    Assert.assertEquals(1, executorCount.get());
    RemoteTaskRunner remoteTaskRunner2 = factory.build();
    Assert.assertEquals(2, executorCount.get());

  }
}
