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

package org.apache.druid.cli;

import org.apache.druid.indexing.overlord.DruidOverlord;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CoordinatorOverlordRedirectInfoTest
{
  private DruidOverlord overlord;
  private DruidCoordinator coordinator;
  private CoordinatorOverlordRedirectInfo redirectInfo;

  @Before
  public void setUp()
  {
    overlord = EasyMock.createMock(DruidOverlord.class);
    coordinator = EasyMock.createMock(DruidCoordinator.class);
    redirectInfo = new CoordinatorOverlordRedirectInfo(overlord, coordinator);
  }

  @Test
  public void testDoLocalIndexerWhenLeading()
  {
    EasyMock.expect(overlord.isLeader()).andReturn(true).anyTimes();
    EasyMock.replay(overlord);
    Assert.assertTrue(redirectInfo.doLocal("/druid/indexer/v1/leader"));
    Assert.assertTrue(redirectInfo.doLocal("/druid/indexer/v1/isLeader"));
    Assert.assertTrue(redirectInfo.doLocal("/druid/indexer/v1/other/path"));
    EasyMock.verify(overlord);
  }
}
