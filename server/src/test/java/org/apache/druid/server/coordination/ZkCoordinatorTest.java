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

package org.apache.druid.server.coordination;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.EnsurePath;
import org.apache.curator.utils.ZKPaths;
import org.apache.druid.server.initialization.BatchDataSegmentAnnouncerConfig;
import org.apache.druid.server.initialization.ZkPathsConfig;
import org.easymock.EasyMock;
import org.junit.Test;

public class ZkCoordinatorTest
{
  private final DruidServerMetadata me = new DruidServerMetadata(
      "dummyServer",
      "dummyHost",
      null,
      0,
      ServerType.HISTORICAL,
      "normal",
      0
  );
  private final ZkPathsConfig zkPaths = new ZkPathsConfig()
  {
    @Override
    public String getBase()
    {
      return "/druid";
    }
  };

  @Test(timeout = 60_000L)
  public void testSegmentPathIsCreatedIfZkAnnouncementIsEnabled() throws Exception
  {
    testSegmentPathCreated(true);
  }

  @Test(timeout = 60_000L)
  public void testSegmentPathIsNotCreatedIfZkAnnouncementIsDisabled() throws Exception
  {
    testSegmentPathCreated(false);
  }

  private void testSegmentPathCreated(boolean announceSegmentsOnZk) throws Exception
  {
    final String liveSegmentsPath = ZKPaths.makePath(
        zkPaths.getLiveSegmentsPath(),
        me.getName()
    );

    final EnsurePath mockEnsurePath = EasyMock.mock(EnsurePath.class);
    final CuratorFramework mockCurator = EasyMock.mock(CuratorFramework.class);

    if (announceSegmentsOnZk) {
      EasyMock.expect(mockCurator.newNamespaceAwareEnsurePath(liveSegmentsPath))
              .andReturn(mockEnsurePath).once();

      EasyMock.expect(mockCurator.getZookeeperClient())
              .andReturn(null).once();

      mockEnsurePath.ensure(EasyMock.anyObject());
      EasyMock.expectLastCall().once();
    }

    EasyMock.replay(mockCurator, mockEnsurePath);
    final ZkCoordinator zkCoordinator = new ZkCoordinator(
        zkPaths,
        me,
        mockCurator,
        new BatchDataSegmentAnnouncerConfig() {
          @Override
          public boolean isSkipSegmentAnnouncementOnZk()
          {
            return !announceSegmentsOnZk;
          }
        }
    );

    zkCoordinator.start();
    EasyMock.verify();
    zkCoordinator.stop();
  }
}
