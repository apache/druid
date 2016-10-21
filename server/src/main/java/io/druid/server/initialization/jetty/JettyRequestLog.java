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

package io.druid.server.initialization.jetty;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.component.AbstractLifeCycle;

import io.druid.java.util.common.logger.Logger;


public class JettyRequestLog extends AbstractLifeCycle implements RequestLog
{
  private final static Logger logger = new Logger("io.druid.jetty.RequestLog");

  @Override
  public void log(Request request, Response response)
  {
    if (logger.isDebugEnabled()) {
      logger.debug(
          "%s %s %s",
          request.getMethod(),
          request.getUri().toString(),
          request.getProtocol().toString()
      );
    }
  }
}

