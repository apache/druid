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

package org.apache.druid.indexing.worker.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingCluster;
import org.apache.druid.curator.PotentiallyGzippedCompressionProvider;
import org.apache.druid.indexing.overlord.config.RemoteTaskRunnerConfig;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.indexing.worker.WorkerCuratorCoordinator;
import org.apache.druid.indexing.worker.WorkerTaskMonitor;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.initialization.IndexerZkConfig;
import org.apache.druid.server.initialization.ZkPathsConfig;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.Response;

/**
 */
public class WorkerResourceTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();
  private static final String BASE_PATH = "/test/druid";
  private static final String ANNOUNCEMENT_PATH = StringUtils.format("%s/indexer/announcements/host", BASE_PATH);

  private TestingCluster testingCluster;
  private CuratorFramework cf;

  private Worker worker;

  private WorkerCuratorCoordinator curatorCoordinator;
  private WorkerResource workerResource;

  @Before
  public void setUp() throws Exception
  {
    testingCluster = new TestingCluster(1);
    testingCluster.start();

    cf = CuratorFrameworkFactory.builder()
                                .connectString(testingCluster.getConnectString())
                                .retryPolicy(new ExponentialBackoffRetry(1, 10))
                                .compressionProvider(new PotentiallyGzippedCompressionProvider(false))
                                .build();
    cf.start();
    cf.blockUntilConnected();
    cf.create().creatingParentsIfNeeded().forPath(BASE_PATH);

    worker = new Worker(
        "http",
        "host",
        "ip",
        3,
        "v1",
        WorkerConfig.DEFAULT_CATEGORY
    );

    curatorCoordinator = new WorkerCuratorCoordinator(
        JSON_MAPPER,
        new IndexerZkConfig(new ZkPathsConfig()
        {
          @Override
          public String getBase()
          {
            return BASE_PATH;
          }
        }, null, null, null, null),
        new RemoteTaskRunnerConfig(),
        cf,
        worker
    );
    curatorCoordinator.start();

    workerResource = new WorkerResource(
        worker,
        curatorCoordinator,
        null,
        EasyMock.createNiceMock(WorkerTaskMonitor.class)
    );
  }

  @After
  public void tearDown() throws Exception
  {
    curatorCoordinator.stop();
    cf.close();
    testingCluster.close();
  }

  @Test
  public void testDoDisable() throws Exception
  {
    Worker theWorker = JSON_MAPPER.readValue(cf.getData().forPath(ANNOUNCEMENT_PATH), Worker.class);
    Assert.assertEquals("v1", theWorker.getVersion());

    Response res = workerResource.doDisable();
    Assert.assertEquals(Response.Status.OK.getStatusCode(), res.getStatus());

    theWorker = JSON_MAPPER.readValue(cf.getData().forPath(ANNOUNCEMENT_PATH), Worker.class);
    Assert.assertTrue(theWorker.getVersion().isEmpty());
  }

  @Test
  public void testDoEnable() throws Exception
  {
    // Disable the worker
    Response res = workerResource.doDisable();
    Assert.assertEquals(Response.Status.OK.getStatusCode(), res.getStatus());
    Worker theWorker = JSON_MAPPER.readValue(cf.getData().forPath(ANNOUNCEMENT_PATH), Worker.class);
    Assert.assertTrue(theWorker.getVersion().isEmpty());

    // Enable the worker
    res = workerResource.doEnable();
    Assert.assertEquals(Response.Status.OK.getStatusCode(), res.getStatus());
    theWorker = JSON_MAPPER.readValue(cf.getData().forPath(ANNOUNCEMENT_PATH), Worker.class);
    Assert.assertEquals("v1", theWorker.getVersion());
  }
}
