/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.server.bridge;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.inject.Inject;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.client.selector.Server;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.guice.annotations.Global;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.SegmentDescriptor;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Interval;

import javax.ws.rs.core.MediaType;
import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 */
public class BridgeQuerySegmentWalker implements QuerySegmentWalker
{
  private static final Logger log = new Logger(BridgeQuerySegmentWalker.class);

  private final ServerDiscoverySelector brokerSelector;
  private final HttpClient httpClient;
  private final ObjectMapper jsonMapper;
  private final StatusResponseHandler responseHandler;

  @Inject
  public BridgeQuerySegmentWalker(
      ServerDiscoverySelector brokerSelector,
      @Global HttpClient httpClient,
      ObjectMapper jsonMapper
  )
  {
    this.brokerSelector = brokerSelector;
    this.httpClient = httpClient;
    this.jsonMapper = jsonMapper;
    this.responseHandler = new StatusResponseHandler(Charsets.UTF_8);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(
      Query<T> query, Iterable<Interval> intervals
  )
  {
    return makeRunner();
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(
      Query<T> query, Iterable<SegmentDescriptor> specs
  )
  {
    return makeRunner();
  }

  private <T> QueryRunner<T> makeRunner()
  {
    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
      {
        try {
          Server instance = brokerSelector.pick();
          if (instance == null) {
            return Sequences.empty();
          }
          final Server brokerServer = brokerSelector.pick();
          final URL url = new URL(
              brokerServer.getScheme(),
              brokerServer.getAddress(),
              brokerServer.getPort(),
              "/druid/v2/"
          );

          StatusResponseHolder response = httpClient.go(
              new Request(
                  HttpMethod.POST,
                  url
              ).setContent(
                  MediaType.APPLICATION_JSON,
                  jsonMapper.writeValueAsBytes(query)
              ),
              responseHandler
          ).get();

          List<T> results = jsonMapper.readValue(
              response.getContent(), new TypeReference<List<T>>()
              {
              }
          );

          return Sequences.simple(results);
        }
        catch (Exception e) {
          log.error(e, "Exception with bridge query");

          return Sequences.empty();
        }
      }
    };
  }
}
