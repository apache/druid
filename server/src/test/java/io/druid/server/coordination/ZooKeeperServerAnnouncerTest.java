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

package io.druid.server.coordination;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.concurrent.Execs;
import io.druid.curator.CuratorTestBase;
import io.druid.curator.announcement.Announcer;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.server.initialization.ZkPathsConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;

/**
 */
public class ZooKeeperServerAnnouncerTest extends CuratorTestBase
{

  private static final ObjectMapper MAPPER = new DefaultObjectMapper();

  private static final DruidServerMetadata ME = new DruidServerMetadata(
      "node",
      "localhost:8080",
      100,
      "sometype",
      "default_tier",
      0,
      "service",
      "localhost",
      8080
  );

  private static final ZkPathsConfig ZK_PATHS_CONFIG = new ZkPathsConfig()
  {
    @Override
    public String getBase()
    {
      return "test";
    }
  };

  private ZooKeeperServerAnnouncer zkServerAnnouncer;
  private Announcer announcer;

  @Before
  public void setup() throws Exception
  {
    setupServerAndCurator();
    curator.start();
    curator.blockUntilConnected();

    announcer = new Announcer(curator, Execs.singleThreaded("test-announcer-sanity-%s"));
    announcer.start();

    zkServerAnnouncer = new ZooKeeperServerAnnouncer(
        ME,
        MAPPER,
        ZK_PATHS_CONFIG,
        announcer,
        new HashSet<String>()
    );
  }

  @Test
  public void testAnnounceAndUnannounceLeadership() throws Exception
  {
    zkServerAnnouncer.start();
    final String leadershipPath = ZK_PATHS_CONFIG.getLeadershipPathForServer(ME);
    Assert.assertNull(curator.checkExists().forPath(leadershipPath));
    zkServerAnnouncer.announceLeadership();
    Assert.assertEquals(
        ME,
        MAPPER.readValue(
            curator.getData().decompressed().forPath(leadershipPath),
            DruidServerMetadata.class
        )
    );
    zkServerAnnouncer.unannounceLeadership();
    Assert.assertNull(curator.checkExists().forPath(leadershipPath));
    zkServerAnnouncer.stop();
  }

  @Test
  public void testAnnounceAndUnannounceSelf() throws Exception
  {
    final String selfAnnouncementPath = ZK_PATHS_CONFIG.getAnnouncementPathForServer(ME);
    Assert.assertNull(curator.checkExists().forPath(selfAnnouncementPath));
    zkServerAnnouncer.announceSelf();
    Assert.assertEquals(
        ME,
        MAPPER.readValue(curator.getData().decompressed().forPath(selfAnnouncementPath), DruidServerMetadata.class)
    );
    zkServerAnnouncer.unannounceSelf();
    Assert.assertNull(curator.checkExists().forPath(selfAnnouncementPath));
  }

  @Test
  public void testServerAnnouncerStartStop() throws Exception
  {
    final String selfAnnouncementPath = ZK_PATHS_CONFIG.getAnnouncementPathForServer(ME);
    Assert.assertNull(curator.checkExists().forPath(selfAnnouncementPath));
    zkServerAnnouncer.start();
    Assert.assertEquals(
        ME,
        MAPPER.readValue(curator.getData().decompressed().forPath(selfAnnouncementPath), DruidServerMetadata.class)
    );
    zkServerAnnouncer.stop();
    Assert.assertNull(curator.checkExists().forPath(selfAnnouncementPath));
  }

  @After
  public void tearDown() throws Exception
  {
    announcer.stop();
    tearDownServerAndCurator();
  }
}