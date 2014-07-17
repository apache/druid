/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.indexing.worker.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.curator.PotentiallyGzippedCompressionProvider;
import io.druid.indexing.overlord.config.RemoteTaskRunnerConfig;
import io.druid.indexing.worker.Worker;
import io.druid.indexing.worker.WorkerCuratorCoordinator;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.server.initialization.ZkPathsConfig;
import junit.framework.Assert;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.Response;

/**
 */
public class WorkerResourceTest
{
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();
  private static final String basePath = "/test/druid";
  private static final String announcementsPath = String.format("%s/indexer/announcements/host", basePath);

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
    cf.create().creatingParentsIfNeeded().forPath(basePath);

    worker = new Worker(
        "host",
        "ip",
        3,
        "v1"
    );

    curatorCoordinator = new WorkerCuratorCoordinator(
        jsonMapper,
        new ZkPathsConfig()
        {
          @Override
          public String getZkBasePath()
          {
            return basePath;
          }
        },
        new RemoteTaskRunnerConfig(),
        cf,
        worker
    );
    curatorCoordinator.start();

    workerResource = new WorkerResource(
        worker,
        curatorCoordinator,
        null
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
    Worker theWorker = jsonMapper.readValue(cf.getData().forPath(announcementsPath), Worker.class);
    Assert.assertEquals("v1", theWorker.getVersion());

    Response res = workerResource.doDisable();
    Assert.assertEquals(Response.Status.OK.getStatusCode(), res.getStatus());

    theWorker = jsonMapper.readValue(cf.getData().forPath(announcementsPath), Worker.class);
    Assert.assertTrue(theWorker.getVersion().isEmpty());
  }

  @Test
  public void testDoEnable() throws Exception
  {
    // Disable the worker
    Response res = workerResource.doDisable();
    Assert.assertEquals(Response.Status.OK.getStatusCode(), res.getStatus());
    Worker theWorker = jsonMapper.readValue(cf.getData().forPath(announcementsPath), Worker.class);
    Assert.assertTrue(theWorker.getVersion().isEmpty());

    // Enable the worker
    res = workerResource.doEnable();
    Assert.assertEquals(Response.Status.OK.getStatusCode(), res.getStatus());
    theWorker = jsonMapper.readValue(cf.getData().forPath(announcementsPath), Worker.class);
    Assert.assertEquals("v1", theWorker.getVersion());
  }
}
