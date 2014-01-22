/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.client.selector.Server;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.guice.annotations.Global;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.SegmentDescriptor;
import org.joda.time.Interval;

import java.net.URL;
import java.util.List;

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
      public Sequence<T> run(Query<T> query)
      {
        try {
          Server instance = brokerSelector.pick();
          if (instance == null) {
            return Sequences.empty();
          }

          final String url = String.format(
              "http://%s/druid/v2/",
              brokerSelector.pick().getHost()
          );

          StatusResponseHolder response = httpClient.post(new URL(url))
                                                    .setContent(
                                                        "application/json",
                                                        jsonMapper.writeValueAsBytes(query)
                                                    )
                                                    .go(responseHandler)
                                                    .get();

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
