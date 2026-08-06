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

package org.apache.druid.server.http;

import org.apache.druid.server.coordinator.DruidCoordinator;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URL;

public class CoordinatorRedirectInfoTest
{
  private DruidCoordinator druidCoordinator;
  private CoordinatorRedirectInfo coordinatorRedirectInfo;

  @BeforeEach
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
    Assertions.assertTrue(coordinatorRedirectInfo.doLocal(null));
    Assertions.assertTrue(coordinatorRedirectInfo.doLocal("/druid/coordinator/v1/leader"));
    Assertions.assertTrue(coordinatorRedirectInfo.doLocal("/druid/coordinator/v1/isLeader"));
    Assertions.assertTrue(coordinatorRedirectInfo.doLocal("/druid/coordinator/v1/other/path"));
    EasyMock.verify(druidCoordinator);
  }

  @Test
  public void testDoLocalWhenNotLeading()
  {
    EasyMock.expect(druidCoordinator.isLeader()).andReturn(false).anyTimes();
    EasyMock.replay(druidCoordinator);
    Assertions.assertFalse(coordinatorRedirectInfo.doLocal(null));
    Assertions.assertTrue(coordinatorRedirectInfo.doLocal("/druid/coordinator/v1/leader"));
    Assertions.assertTrue(coordinatorRedirectInfo.doLocal("/druid/coordinator/v1/isLeader"));
    Assertions.assertFalse(coordinatorRedirectInfo.doLocal("/druid/coordinator/v1/other/path"));
    EasyMock.verify(druidCoordinator);
  }

  @Test
  public void testGetRedirectURLNull()
  {
    EasyMock.expect(druidCoordinator.getCurrentLeader()).andReturn(null).anyTimes();
    EasyMock.replay(druidCoordinator);
    URL url = coordinatorRedirectInfo.getRedirectURL("query", "/request");
    Assertions.assertNull(url);
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
    Assertions.assertEquals("http://localhost/request?foo=bar&x=y", url.toString());
    EasyMock.verify(druidCoordinator);
  }
}
