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

package org.apache.druid.tests.query;

import com.google.inject.Inject;
import org.apache.druid.testing.clients.OverlordResourceTestClient;
import org.apache.druid.testing.clients.ResponseException;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.tests.TestNGGroup;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.testng.Assert;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.util.function.Consumer;

@Test(groups = TestNGGroup.HTTP_ENDPOINT)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITOverlordResourceTest
{
  @Inject
  protected OverlordResourceTestClient indexer;

  @Test
  public void testGetSupervisorStatusNotFound()
  {
    callAndCheckNotFound(indexer::getSupervisorStatus);
  }

  @Test
  public void testGetSupervisorHistoryNotFound()
  {
    callAndCheckNotFound(indexer::getSupervisorHistory);
  }

  @Test
  public void testResumeSupervisorNotFound()
  {
    callAndCheckNotFound(indexer::resumeSupervisor);
  }

  @Test
  public void testSuspendSupervisorNotFound()
  {
    callAndCheckNotFound(indexer::suspendSupervisor);
  }

  @Test
  public void testShutdownSupervisorNotFound()
  {
    callAndCheckNotFound(indexer::shutdownSupervisor);
  }

  @Test
  public void testTerminateSupervisorNotFound()
  {
    callAndCheckNotFound(indexer::terminateSupervisor);
  }

  @Test
  public void testGetSupervisorHealthNotFound()
  {
    callAndCheckNotFound(indexer::getSupervisorHealth);
  }

  @Test
  public void testStatsSupervisorNotFound()
  {
    callAndCheckNotFound(indexer::statsSupervisor);
  }

  @Test
  public void testResetSupervisorNotFound()
  {
    callAndCheckNotFound(indexer::resetSupervisor);
  }

  @Test
  public void testGetTaskStatusNotFound()
  {
    callAndCheckNotFound(indexer::getTaskStatus);
  }

  @Test
  public void testShutdownTaskNotFound()
  {
    callAndCheckNotFound(indexer::shutdownTask);
  }

  @Test
  public void testGetTaskLogNotFound()
  {
    callAndCheckNotFound(indexer::getTaskLog);
  }

  @Test
  public void testGetTaskReportNotFound()
  {
    callAndCheckNotFound(indexer::getTaskReport);
  }

  @Test
  public void testGetTaskPayLoadNotFound()
  {
    callAndCheckNotFound(indexer::getTaskPayload);
  }

  private void callAndCheckNotFound(Consumer<String> runnable)
  {
    String supervisorId = "not_exist_id";
    try {
      runnable.accept(supervisorId);
    }
    catch (ResponseException e) {
      Assert.assertEquals(HttpResponseStatus.NOT_FOUND.getCode(), e.getResponse().getStatus().getCode());
      ResponseException.ErrorContent content = e.bodyToObject(ResponseException.ErrorContent.class);
      Assert.assertTrue(content.getError().contains(supervisorId));
      return;
    }
    Assert.fail("Should not go to here");
  }
}
