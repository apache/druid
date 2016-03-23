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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.InputStreamResponseHandler;
import io.druid.client.selector.Server;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.granularity.QueryGranularity;
import io.druid.guice.annotations.Global;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.server.coordinator.helper.DruidCoordinatorHadoopSegmentMerger;
import io.druid.timeline.DataSegment;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class IndexingServiceClient
{
  private static final Logger log = new Logger(IndexingServiceClient.class);
  private static final InputStreamResponseHandler RESPONSE_HANDLER = new InputStreamResponseHandler();
  private final static String INCOMPLETE_TASKS = "incompleteTasks";

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

    postTask(new ClientAppendQuery(dataSource, segments));
  }

  public String hadoopMergeSegments(
      String dataSource,
      List<Interval> intervalsToReindex,
      AggregatorFactory[] aggregators,
      QueryGranularity queryGranularity,
      List<String> dimensions,
      Map<String, Object> tuningConfig,
      List<String> hadoopCoordinates
  )
  {
    final InputStream queryResponse = postTask(
        new ClientHadoopIndexQuery(
            String.format(
                "%s_%s_%s",
                DruidCoordinatorHadoopSegmentMerger.HADOOP_REINDEX_TASK_ID_PREFIX, dataSource, new DateTime()
            ),
            dataSource,
            intervalsToReindex,
            aggregators,
            dimensions,
            queryGranularity,
            tuningConfig,
            hadoopCoordinates,
            jsonMapper
        )
    );

    try {
      return jsonMapper.<Map<String, String>>readValue(
          queryResponse, new TypeReference<Map<String, String>>()
          {
          }
      ).get("task");
    }
    catch (IOException e) {
      log.warn(
          "Error pasring HadoopReindexTask ID for dataSource [%s] and Interval [%s]",
          dataSource,
          intervalsToReindex
      );
      return null;
    }
  }

  public void killSegments(String dataSource, Interval interval)
  {
    postTask(new ClientKillQuery(dataSource, interval));
  }

  public void upgradeSegment(DataSegment dataSegment)
  {
    postTask(new ClientConversionQuery(dataSegment));
  }

  public void upgradeSegments(String dataSource, Interval interval)
  {
    postTask(new ClientConversionQuery(dataSource, interval));
  }

  public Map<String, Object> getSegmentMetadata(String dataSource, List<Interval> intervals)
  {
    postTask(new ClientSegmentMetadataQuery(dataSource, intervals));
    return null;
  }

  public List<Map<String, Object>> getIncompleteTasks()
  {
    final InputStream queryResponse = runGetQuery(INCOMPLETE_TASKS);
    try {
      return jsonMapper.readValue(
          queryResponse, new TypeReference<List<Map<String, Object>>>()
          {
          }
      );
    }
    catch (IOException e) {
      log.warn("Error parsing response for incompleteTasks query from Overlord [%s]", baseUrl());
      return null;
    }
  }

  private InputStream runGetQuery(String path)
  {
    try {
      return client.go(
          new Request(
              HttpMethod.GET,
              new URL(String.format("%s/%s", baseUrl(), path))
          ), RESPONSE_HANDLER
      ).get();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private InputStream postTask(Object queryObject)
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
