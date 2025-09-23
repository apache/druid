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
import org.apache.calcite.avatica.metrics.Timer;
import org.apache.calcite.avatica.remote.ProtobufHandler;
import org.apache.calcite.avatica.remote.ProtobufTranslation;
import org.apache.calcite.avatica.remote.ProtobufTranslationImpl;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.server.AvaticaProtobufHandler;
import org.apache.calcite.avatica.util.UnsynchronizedBuffer;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.DruidNode;
import org.eclipse.jetty.io.Content;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.Callback;

import javax.inject.Inject;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class DruidAvaticaProtobufHandler extends DruidAvaticaHandler
{

  private static final Logger LOG = new Logger(DruidAvaticaProtobufHandler.class);

  public static final String AVATICA_PATH_NO_TRAILING_SLASH = "/druid/v2/sql/avatica-protobuf";
  public static final String AVATICA_PATH = AVATICA_PATH_NO_TRAILING_SLASH + "/";

  private final ProtobufHandler protobufHandler;

  @Inject
  public DruidAvaticaProtobufHandler(
      final DruidMeta druidMeta,
      @Self final DruidNode druidNode,
      final AvaticaMonitor metrics
  )
  {
    super(druidMeta, metrics, AvaticaProtobufHandler.class);
    ProtobufTranslation protobufTranslation = new ProtobufTranslationImpl();
    this.protobufHandler = new ProtobufHandler(service, protobufTranslation, this.metrics);
    setServerRpcMetadata(new Service.RpcMetadataResponse(druidNode.getHostAndPortToUse()));
  }

  @Override
  public boolean handle(Request request, Response response, Callback callback) throws Exception
  {
    String requestURI = request.getHttpURI().getPath();
    if (AVATICA_PATH_NO_TRAILING_SLASH.equals(StringUtils.maybeRemoveTrailingSlash(requestURI))) {
      try (Timer.Context ctx = this.requestTimer.start()) {
        if (!"POST".equals(request.getMethod())) {
          response.setStatus(405);
          response.write(
              true,
              ByteBuffer.wrap("This server expects only POST calls.".getBytes(StandardCharsets.UTF_8)), callback
          );
          return true;
        }
        final byte[] requestBytes;
        // Avoid a new buffer creation for every HTTP request
        final UnsynchronizedBuffer buffer = threadLocalBuffer.get();
        try (InputStream inputStream = Content.Source.asInputStream(request)) {
          requestBytes = AvaticaUtils.readFullyToBytes(inputStream, buffer);
        }
        finally {
          buffer.reset();
        }

        response.getHeaders().put("Content-Type", "application/octet-stream;charset=utf-8");

        org.apache.calcite.avatica.remote.Handler.HandlerResponse<byte[]> handlerResponse;
        try {
          handlerResponse = protobufHandler.apply(requestBytes);
        }
        catch (Exception e) {
          LOG.debug(e, "Error invoking request");
          handlerResponse = protobufHandler.convertToErrorResponse(e);
        }

        response.setStatus(handlerResponse.getStatusCode());
        response.write(true, ByteBuffer.wrap(handlerResponse.getResponse()), callback);
        return true;
      }
    }
    return false;
  }

  @Override
  public void setServerRpcMetadata(Service.RpcMetadataResponse metadata)
  {
    super.setServerRpcMetadata(metadata);
    if (protobufHandler != null) {
      protobufHandler.setRpcMetadata(metadata);
    }
  }
}
