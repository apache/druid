/*
 * Druid - a distributed column store.
 * Copyright 2015 - Yahoo! Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
  public void testDoLocal()
  {
    EasyMock.expect(druidCoordinator.isLeader()).andReturn(true).anyTimes();
    EasyMock.replay(druidCoordinator);
    Assert.assertTrue(coordinatorRedirectInfo.doLocal());
    EasyMock.verify(druidCoordinator);
  }

  @Test
  public void testGetRedirectURLNull()
  {
    EasyMock.expect(druidCoordinator.getCurrentLeader()).andReturn(null).anyTimes();
    EasyMock.replay(druidCoordinator);
    URL url = coordinatorRedirectInfo.getRedirectURL("query", "request");
    Assert.assertNull(url);
  }
  @Test
  public void testGetRedirectURL()
  {
    String host = "localhost";
    String query = "query";
    String request = "request";
    EasyMock.expect(druidCoordinator.getCurrentLeader()).andReturn(host).anyTimes();
    EasyMock.replay(druidCoordinator);
    URL url = coordinatorRedirectInfo.getRedirectURL(query,request);
    Assert.assertTrue(url.toString().contains(host+request+"?"+query));
  }
}