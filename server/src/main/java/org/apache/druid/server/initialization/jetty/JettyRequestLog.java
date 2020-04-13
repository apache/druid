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

package org.apache.druid.server.initialization.jetty;

import org.apache.druid.java.util.common.logger.Logger;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.component.AbstractLifeCycle;


public class JettyRequestLog extends AbstractLifeCycle implements RequestLog
{
  private static final Logger logger = new Logger("org.apache.druid.jetty.RequestLog");

  @Override
  public void log(Request request, Response response)
  {
    if (logger.isDebugEnabled()) {
      logger.debug(
          "%s %s %s %s",
          request.getRemoteAddr(),
          request.getMethod(),
          request.getHttpURI().toString(),
          request.getProtocol()
      );
    }
  }
}

