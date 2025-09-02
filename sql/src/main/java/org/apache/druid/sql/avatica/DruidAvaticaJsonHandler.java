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

import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.avatica.metrics.MetricsSystem;
import org.apache.calcite.avatica.metrics.Timer;
import org.apache.calcite.avatica.remote.JsonHandler;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.remote.MetricsHelper;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.server.AvaticaJsonHandler;
import org.apache.calcite.avatica.server.MetricsAwareAvaticaHandler;
import org.apache.calcite.avatica.util.UnsynchronizedBuffer;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.DruidNode;
import org.eclipse.jetty.io.Content;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.Callback;

import javax.inject.Inject;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class DruidAvaticaJsonHandler extends Handler.Abstract implements MetricsAwareAvaticaHandler
{
  private static final Logger LOG = new Logger(DruidAvaticaJsonHandler.class);

  public static final String AVATICA_PATH_NO_TRAILING_SLASH = "/druid/v2/sql/avatica";
  public static final String AVATICA_PATH = AVATICA_PATH_NO_TRAILING_SLASH + "/";


  private final Service service;
  private final MetricsSystem metrics;
  private final JsonHandler jsonHandler;
  private final Timer requestTimer;
  private final ThreadLocal<UnsynchronizedBuffer> threadLocalBuffer;

  @Inject
  public DruidAvaticaJsonHandler(
      final DruidMeta druidMeta,
      @Self final DruidNode druidNode,
      final AvaticaMonitor avaticaMonitor
  )
  {
    super();
    this.service = new LocalService(druidMeta);
    this.metrics = Objects.requireNonNull(avaticaMonitor);
    this.jsonHandler = new JsonHandler(service, this.metrics);

    // Metrics
    this.requestTimer = this.metrics.getTimer(
        MetricsHelper.concat(AvaticaJsonHandler.class, MetricsAwareAvaticaHandler.REQUEST_TIMER_NAME));

    this.threadLocalBuffer = ThreadLocal.withInitial(UnsynchronizedBuffer::new);

    setServerRpcMetadata(new Service.RpcMetadataResponse(druidNode.getHostAndPortToUse()));
  }

  @Override
  public boolean handle(Request request, Response response, Callback callback) throws Exception
  {
    String requestURI = request.getHttpURI().getPath();
    try(Timer.Context ctx = this.requestTimer.start()) {
      if (AVATICA_PATH_NO_TRAILING_SLASH.equals(StringUtils.maybeRemoveTrailingSlash(requestURI))) {
        response.getHeaders().put("Content-Type", "application/json;charset=utf-8");

        if (!"POST".equals(request.getMethod())) {
          response.setStatus(405);
          response.write(
              true,
              ByteBuffer.wrap("This server expects only POST calls.".getBytes(StandardCharsets.UTF_8)), callback
          );
          return true;
        }

        String rawRequest = request.getHeaders().get("request");
        if (rawRequest == null) {
          // Avoid a new buffer creation for every HTTP request
          final UnsynchronizedBuffer buffer = threadLocalBuffer.get();
          try (InputStream inputStream = Content.Source.asInputStream(request)) {
            byte[] bytes = AvaticaUtils.readFullyToBytes(inputStream, buffer);
            String encoding = request.getHeaders().get("Content-Encoding");
            if (encoding == null) {
              encoding = "UTF-8";
            }
            rawRequest = AvaticaUtils.newString(bytes, encoding);
          }
          finally {
            // Reset the offset into the buffer after we're done
            buffer.reset();
          }
        }
        final String jsonRequest = rawRequest;
        LOG.trace("request: %s", jsonRequest);

        org.apache.calcite.avatica.remote.Handler.HandlerResponse<String> jsonResponse;
        try {
          jsonResponse = jsonHandler.apply(jsonRequest);
        }
        catch (Exception e) {
          LOG.debug(e, "Error invoking request");
          jsonResponse = jsonHandler.convertToErrorResponse(e);
        }

        LOG.trace("response: %s", jsonResponse);
        response.setStatus(jsonResponse.getStatusCode());
        response.write(true, ByteBuffer.wrap(jsonResponse.getResponse().getBytes(StandardCharsets.UTF_8)), callback);
        return true;
      }
    }
    return false;
  }

  @Override
  public MetricsSystem getMetrics()
  {
    return metrics;
  }

  @Override
  public void setServerRpcMetadata(Service.RpcMetadataResponse metadata)
  {
    service.setRpcMetadata(metadata);
    jsonHandler.setRpcMetadata(metadata);
  }
}
