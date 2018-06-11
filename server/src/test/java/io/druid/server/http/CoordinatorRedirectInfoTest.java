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

package io.druid.server.http;

import io.druid.server.coordinator.DruidCoordinator;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;

public class CoordinatorRedirectInfoTest
{
  private DruidCoordinator druidCoordinator;
  private CoordinatorRedirectInfo coordinatorRedirectInfo;

  @Before
  public void setUp()
  {
    druidCoordinator = EasyMock.createMock(DruidCoordinator.class);
    coordinatorRedirectInfo = new CoordinatorRedirectInfo(druidCoordinator);
  }

  @Test
  public void testDoLocalWhenLeading()
  {
    EasyMock.expect(druidCoordinator.isLeader()).andReturn(true).anyTimes();
    EasyMock.replay(druidCoordinator);
    Assert.assertTrue(coordinatorRedirectInfo.doLocal(null));
    Assert.assertTrue(coordinatorRedirectInfo.doLocal("/druid/coordinator/v1/leader"));
    Assert.assertTrue(coordinatorRedirectInfo.doLocal("/druid/coordinator/v1/isLeader"));
    Assert.assertTrue(coordinatorRedirectInfo.doLocal("/druid/coordinator/v1/other/path"));
    EasyMock.verify(druidCoordinator);
  }

  @Test
  public void testDoLocalWhenNotLeading()
  {
    EasyMock.expect(druidCoordinator.isLeader()).andReturn(false).anyTimes();
    EasyMock.replay(druidCoordinator);
    Assert.assertFalse(coordinatorRedirectInfo.doLocal(null));
    Assert.assertTrue(coordinatorRedirectInfo.doLocal("/druid/coordinator/v1/leader"));
    Assert.assertTrue(coordinatorRedirectInfo.doLocal("/druid/coordinator/v1/isLeader"));
    Assert.assertFalse(coordinatorRedirectInfo.doLocal("/druid/coordinator/v1/other/path"));
    EasyMock.verify(druidCoordinator);
  }

  @Test
  public void testGetRedirectURLNull()
  {
    EasyMock.expect(druidCoordinator.getCurrentLeader()).andReturn(null).anyTimes();
    EasyMock.replay(druidCoordinator);
    URL url = coordinatorRedirectInfo.getRedirectURL("query", "/request");
    Assert.assertNull(url);
    EasyMock.verify(druidCoordinator);
  }

  @Test
  public void testGetRedirectURL()
  {
    String query = "foo=bar&x=y";
    String request = "/request";
    EasyMock.expect(druidCoordinator.getCurrentLeader()).andReturn("http://localhost").anyTimes();
    EasyMock.replay(druidCoordinator);
    URL url = coordinatorRedirectInfo.getRedirectURL(query, request);
    Assert.assertEquals("http://localhost/request?foo=bar&x=y", url.toString());
    EasyMock.verify(druidCoordinator);
  }
}
