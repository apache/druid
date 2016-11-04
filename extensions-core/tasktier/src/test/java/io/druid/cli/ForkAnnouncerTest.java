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

package io.druid.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.curator.announcement.Announcer;
import io.druid.indexing.common.task.NoopTask;
import io.druid.indexing.overlord.config.TierForkZkConfig;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.server.DruidNode;
import io.druid.server.initialization.ZkPathsConfig;
import org.easymock.EasyMock;
import org.junit.Test;

public class ForkAnnouncerTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();
  private static final DruidNode SELF = new DruidNode("myService", "myHost", 999);
  private static final String TASK_ID = "test_task_id";

  @Test
  public void testAnnounceTask() throws Exception
  {
    final Announcer announcer = EasyMock.createStrictMock(Announcer.class);
    final TierForkZkConfig zkConfig = new TierForkZkConfig(new ZkPathsConfig(), null, null);
    final String path = zkConfig.getTierTaskIDPath(TASK_ID);
    announcer.announce(EasyMock.eq(path), EasyMock.aryEq(MAPPER.writeValueAsBytes(SELF)));
    EasyMock.expectLastCall().once();
    EasyMock.replay(announcer);
    final ForkAnnouncer forkAnnouncer = new ForkAnnouncer(
        announcer,
        SELF,
        MAPPER,
        zkConfig,
        new NoopTask(TASK_ID, 0, 0, "YES", null, null)
    );
    forkAnnouncer.announceTask();
    EasyMock.verify(announcer);
  }

  @Test
  public void testUnannounceTask() throws Exception
  {
    final Announcer announcer = EasyMock.createStrictMock(Announcer.class);
    final TierForkZkConfig zkConfig = new TierForkZkConfig(new ZkPathsConfig(), null, null);
    final String path = zkConfig.getTierTaskIDPath(TASK_ID);
    announcer.unannounce(EasyMock.eq(path));
    EasyMock.expectLastCall().once();
    EasyMock.replay(announcer);
    final ForkAnnouncer forkAnnouncer = new ForkAnnouncer(
        announcer,
        SELF,
        MAPPER,
        zkConfig,
        new NoopTask(TASK_ID, 0, 0, "YES", null, null)
    );
    forkAnnouncer.unannounceTask();
    EasyMock.verify(announcer);
  }
}

