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

package org.apache.druid.testing.clients;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.inject.Inject;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHolder;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.testing.guice.TestClient;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.apache.druid.utils.Throwables;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractQueryResourceTestClient<QueryType>
{
  private static final Logger LOG = new Logger(AbstractQueryResourceTestClient.class);

  final String contentTypeHeader;

  /**
   * a 'null' means the Content-Type in response defaults to Content-Type of request
   */
  final String acceptHeader;

  final ObjectMapper jsonMapper;
  final ObjectMapper smileMapper;
  final HttpClient httpClient;
  final String routerUrl;
  final Map<String, EncoderDecoder> encoderDecoderMap;

  /**
   * A encoder/decoder that encodes/decodes requests/responses based on Content-Type.
   */
  interface EncoderDecoder
  {
    byte[] encode(Object content) throws IOException;

    List<Map<String, Object>> decode(byte[] content) throws IOException;
  }

  static class ObjectMapperEncoderDecoder implements EncoderDecoder
  {
    private final ObjectMapper om;

    ObjectMapperEncoderDecoder(ObjectMapper om)
    {
      this.om = om;
    }

    @Override
    public byte[] encode(Object content) throws IOException
    {
      return om.writeValueAsBytes(content);
    }

    @Override
    public List<Map<String, Object>> decode(byte[] content) throws IOException
    {
      return om.readValue(content, new TypeReference<List<Map<String, Object>>>()
      {
      });
    }
  }

  /**
   * @param contentTypeHeader Content-Type header of HTTP request
   * @param acceptHeader Accept header of HTTP request. If it's null, Content-Type in response defaults to Content-Type in request
   */
  @Inject
  AbstractQueryResourceTestClient(
      ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      @TestClient HttpClient httpClient,
      String routerUrl,
      String contentTypeHeader,
      @Nullable String acceptHeader
  )
  {
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.httpClient = httpClient;
    this.routerUrl = routerUrl;

    this.encoderDecoderMap = new HashMap<>();
    this.encoderDecoderMap.put(MediaType.APPLICATION_JSON, new ObjectMapperEncoderDecoder(jsonMapper));
    this.encoderDecoderMap.put(SmileMediaTypes.APPLICATION_JACKSON_SMILE, new ObjectMapperEncoderDecoder(smileMapper));

    if (!this.encoderDecoderMap.containsKey(contentTypeHeader)) {
      throw new IAE("Invalid Content-Type[%s]", contentTypeHeader);
    }
    this.contentTypeHeader = contentTypeHeader;

    if (acceptHeader != null) {
      if (!this.encoderDecoderMap.containsKey(acceptHeader)) {
        throw new IAE("Invalid Accept[%s]", acceptHeader);
      }
    }
    this.acceptHeader = acceptHeader;
  }

  public List<Map<String, Object>> query(String url, QueryType query)
  {
    try {
      String expectedResponseType = this.contentTypeHeader;

      Request request = new Request(HttpMethod.POST, new URL(url));
      request.setContent(this.contentTypeHeader, encoderDecoderMap.get(this.contentTypeHeader).encode(query));
      if (this.acceptHeader != null) {
        expectedResponseType = this.acceptHeader;
        request.addHeader(HttpHeaders.Names.ACCEPT, this.acceptHeader);
      }

      final AtomicReference<BytesFullResponseHolder> responseRef = new AtomicReference<>();

      ITRetryUtil.retryUntil(() -> {
        try {
          responseRef.set(httpClient.go(
              request,
              new BytesFullResponseHandler()
          ).get());
        }
        catch (Throwable t) {
          ChannelException ce = Throwables.getCauseOfType(t, ChannelException.class);
          if (ce != null) {
            LOG.info(ce, "Encountered a channel exception. Retrying the query request");
            return false;
          }
        }
        return true;
      },
          true,
          1000,
          3,
          "waiting for queries to complete");

      BytesFullResponseHolder response = responseRef.get();

      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error while querying[%s] status[%s] content[%s]",
            url,
            response.getStatus(),
            new String(response.getContent(), StandardCharsets.UTF_8)
        );
      }

      String responseType = response.getResponse().headers().get(HttpHeaders.Names.CONTENT_TYPE);
      if (!expectedResponseType.equals(responseType)) {
        throw new ISE(
            "Content-Type[%s] in HTTP response does not match the expected[%s]",
            responseType,
            expectedResponseType
        );
      }

      return this.encoderDecoderMap.get(responseType).decode(response.getContent());
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public Future<StatusResponseHolder> queryAsync(String url, QueryType query)
  {
    return queryAsync(url, query, null, null);
  }

  public Future<StatusResponseHolder> queryAsync(String url, QueryType query, String username, String password)
  {
    try {
      Request request = new Request(HttpMethod.POST, new URL(url));
      request.setContent(MediaType.APPLICATION_JSON, encoderDecoderMap.get(MediaType.APPLICATION_JSON).encode(query));
      request.setBasicAuthentication(username, password);
      return httpClient.go(
          request,
          StatusResponseHandler.getInstance()
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public HttpResponseStatus cancelQuery(String url, long timeoutMs)
  {
    try {
      Request request = new Request(HttpMethod.DELETE, new URL(url));
      Future<StatusResponseHolder> future = httpClient.go(
          request,
          StatusResponseHandler.getInstance()
      );
      StatusResponseHolder responseHolder = future.get(timeoutMs, TimeUnit.MILLISECONDS);
      return responseHolder.getStatus();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
