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

package org.apache.druid.msq.indexing.client;

import org.apache.druid.indexer.report.KillTaskReport;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.msq.exec.Controller;
import org.apache.druid.server.security.AuthorizerMapper;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;

public class ControllerChatHandlerTest
{
  private static final String DATASOURCE = "wiki";

  @Test
  public void testHttpGetLiveReports()
  {
    final Controller controller = Mockito.mock(Controller.class);

    TaskReport.ReportMap reportMap = new TaskReport.ReportMap();
    reportMap.put("killUnusedSegments", new KillTaskReport("kill_1", new KillTaskReport.Stats(1, 2, 3)));

    Mockito.when(controller.liveReports())
           .thenReturn(reportMap);

    final AuthorizerMapper authorizerMapper = new AuthorizerMapper(null);
    ControllerChatHandler chatHandler = new ControllerChatHandler(controller, DATASOURCE, authorizerMapper);

    HttpServletRequest httpRequest = Mockito.mock(HttpServletRequest.class);
    Mockito.when(httpRequest.getAttribute(ArgumentMatchers.anyString()))
           .thenReturn("allow-all");
    Response response = chatHandler.httpGetLiveReports(httpRequest);

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(reportMap, response.getEntity());
  }
}
