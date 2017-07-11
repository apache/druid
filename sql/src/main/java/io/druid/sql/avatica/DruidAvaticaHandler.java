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

package io.druid.sql.avatica;

import com.google.inject.Inject;
import io.druid.guice.annotations.Self;
import io.druid.server.DruidNode;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.server.AvaticaJsonHandler;
import org.eclipse.jetty.server.Request;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

public class DruidAvaticaHandler extends AvaticaJsonHandler
{
  static final String AVATICA_PATH = "/druid/v2/sql/avatica/";

  @Inject
  public DruidAvaticaHandler(
      final DruidMeta druidMeta,
      @Self final DruidNode druidNode,
      final AvaticaMonitor avaticaMonitor
  ) throws InstantiationException, IllegalAccessException, InvocationTargetException
  {
    super(new LocalService(druidMeta), avaticaMonitor);
    setServerRpcMetadata(new Service.RpcMetadataResponse(druidNode.getHostAndPortToUse()));
  }

  @Override
  public void handle(
      final String target,
      final Request baseRequest,
      final HttpServletRequest request,
      final HttpServletResponse response
  ) throws IOException, ServletException
  {
    // This is not integrated with the experimental authorization framework.
    // (Non-trivial since we don't know the dataSources up-front)

    if (request.getRequestURI().equals(AVATICA_PATH)) {
      super.handle(target, baseRequest, request, response);
    }
  }
}
