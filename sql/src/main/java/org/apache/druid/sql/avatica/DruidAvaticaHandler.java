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

package org.apache.druid.sql.avatica;

import com.google.inject.Inject;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.server.AvaticaJsonHandler;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.server.DruidNode;
import org.eclipse.jetty.server.Request;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class DruidAvaticaHandler extends AvaticaJsonHandler
{
  public static final String AVATICA_PATH = "/druid/v2/sql/avatica/";

  @Inject
  public DruidAvaticaHandler(
      final DruidMeta druidMeta,
      @Self final DruidNode druidNode,
      final AvaticaMonitor avaticaMonitor
  )
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
    if (request.getRequestURI().equals(AVATICA_PATH)) {
      super.handle(target, baseRequest, request, response);
    }
  }
}
