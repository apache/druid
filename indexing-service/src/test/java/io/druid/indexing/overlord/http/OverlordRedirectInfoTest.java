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

package io.druid.indexing.overlord.http;

import io.druid.indexing.overlord.TaskMaster;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;

public class OverlordRedirectInfoTest
{
  private TaskMaster taskMaster;
  private OverlordRedirectInfo redirectInfo;

  @Before
  public void setUp()
  {
    taskMaster = EasyMock.createMock(TaskMaster.class);
    redirectInfo = new OverlordRedirectInfo(taskMaster);
  }

  @Test
  public void testDoLocal()
  {
    EasyMock.expect(taskMaster.isLeading()).andReturn(true).anyTimes();
    EasyMock.replay(taskMaster);
    Assert.assertTrue(redirectInfo.doLocal());
    EasyMock.verify(taskMaster);
  }

  @Test
  public void testGetRedirectURLNull()
  {
    EasyMock.expect(taskMaster.getLeader()).andReturn(null).anyTimes();
    EasyMock.replay(taskMaster);
    URL url = redirectInfo.getRedirectURL("query", "/request");
    Assert.assertNull(url);
    EasyMock.verify(taskMaster);
  }

  @Test
  public void testGetRedirectURLEmpty()
  {
    EasyMock.expect(taskMaster.getLeader()).andReturn("").anyTimes();
    EasyMock.replay(taskMaster);
    URL url = redirectInfo.getRedirectURL("query", "/request");
    Assert.assertNull(url);
    EasyMock.verify(taskMaster);
  }

  @Test
  public void testGetRedirectURL()
  {
    String host = "localhost";
    String query = "foo=bar&x=y";
    String request = "/request";
    EasyMock.expect(taskMaster.getLeader()).andReturn(host).anyTimes();
    EasyMock.replay(taskMaster);
    URL url = redirectInfo.getRedirectURL(query, request);
    Assert.assertEquals("http://localhost/request?foo=bar&x=y", url.toString());
    EasyMock.verify(taskMaster);
  }
}
