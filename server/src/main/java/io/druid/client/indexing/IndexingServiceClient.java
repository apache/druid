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

package io.druid.client.indexing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.InputStreamResponseHandler;
import io.druid.client.selector.Server;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.guice.annotations.Global;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.timeline.DataSegment;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Interval;

import javax.ws.rs.core.MediaType;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.Iterator;
import java.util.List;

public class IndexingServiceClient
{
  private static final InputStreamResponseHandler RESPONSE_HANDLER = new InputStreamResponseHandler();

  private final HttpClient client;
  private final ObjectMapper jsonMapper;
  private final ServerDiscoverySelector selector;

  @Inject
  public IndexingServiceClient(
      @Global HttpClient client,
      ObjectMapper jsonMapper,
      @IndexingService ServerDiscoverySelector selector
  )
  {
    this.client = client;
    this.jsonMapper = jsonMapper;
    this.selector = selector;
  }

  public void mergeSegments(List<DataSegment> segments)
  {
    final Iterator<DataSegment> segmentsIter = segments.iterator();
    if (!segmentsIter.hasNext()) {
      return;
    }

    final String dataSource = segmentsIter.next().getDataSource();
    while (segmentsIter.hasNext()) {
      DataSegment next = segmentsIter.next();
      if (!dataSource.equals(next.getDataSource())) {
        throw new IAE("Cannot merge segments of different dataSources[%s] and [%s]", dataSource, next.getDataSource());
      }
    }

    runQuery(new ClientAppendQuery(dataSource, segments));
  }

  public void killSegments(String dataSource, Interval interval)
  {
    runQuery(new ClientKillQuery(dataSource, interval));
  }

  public void upgradeSegment(DataSegment dataSegment)
  {
    runQuery(new ClientConversionQuery(dataSegment));
  }

  public void upgradeSegments(String dataSource, Interval interval)
  {
    runQuery(new ClientConversionQuery(dataSource, interval));
  }

  private InputStream runQuery(Object queryObject)
  {
    try {
      return client.go(
          new Request(
              HttpMethod.POST,
              new URL(String.format("%s/task", baseUrl()))
          ).setContent(MediaType.APPLICATION_JSON, jsonMapper.writeValueAsBytes(queryObject)),
          RESPONSE_HANDLER
      ).get();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private String baseUrl()
  {
    try {
      final Server instance = selector.pick();
      if (instance == null) {
        throw new ISE("Cannot find instance of indexingService");
      }

      return new URI(
          instance.getScheme(),
          null,
          instance.getAddress(),
          instance.getPort(),
          "/druid/indexer/v1",
          null,
          null
      ).toString();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
