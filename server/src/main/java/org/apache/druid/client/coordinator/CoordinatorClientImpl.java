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

package org.apache.druid.client.coordinator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.client.ImmutableSegmentLoadInfo;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.ServiceClient;
import org.apache.druid.rpc.ServiceRetryPolicy;
import org.apache.druid.segment.metadata.DataSourceInformation;
import org.apache.druid.timeline.DataSegment;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class CoordinatorClientImpl implements CoordinatorClient
{
  private final ServiceClient client;
  private final ObjectMapper jsonMapper;

  public CoordinatorClientImpl(
      final ServiceClient client,
      final ObjectMapper jsonMapper
  )
  {
    this.client = client;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public ListenableFuture<Boolean> isHandoffComplete(String dataSource, SegmentDescriptor descriptor)
  {
    final String path = StringUtils.format(
        "/druid/coordinator/v1/datasources/%s/handoffComplete?interval=%s&partitionNumber=%d&version=%s",
        StringUtils.urlEncode(dataSource),
        StringUtils.urlEncode(descriptor.getInterval().toString()),
        descriptor.getPartitionNumber(),
        StringUtils.urlEncode(descriptor.getVersion())
    );

    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.GET, path),
            new BytesFullResponseHandler()
        ),
        holder -> JacksonUtils.readValue(jsonMapper, holder.getContent(), Boolean.class)
    );
  }

  @Override
  public ListenableFuture<DataSegment> fetchSegment(String dataSource, String segmentId, boolean includeUnused)
  {
    final String path = StringUtils.format(
        "/druid/coordinator/v1/metadata/datasources/%s/segments/%s?includeUnused=%s",
        StringUtils.urlEncode(dataSource),
        StringUtils.urlEncode(segmentId),
        includeUnused ? "true" : "false"
    );

    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.GET, path),
            new BytesFullResponseHandler()
        ),
        holder -> JacksonUtils.readValue(jsonMapper, holder.getContent(), DataSegment.class)
    );
  }

  @Override
  public Iterable<ImmutableSegmentLoadInfo> fetchServerViewSegments(String dataSource, List<Interval> intervals)
  {
    ArrayList<ImmutableSegmentLoadInfo> retVal = new ArrayList<>();
    for (Interval interval : intervals) {
      String intervalString = StringUtils.replace(interval.toString(), "/", "_");

      final String path = StringUtils.format(
          "/druid/coordinator/v1/datasources/%s/intervals/%s/serverview?full",
          StringUtils.urlEncode(dataSource),
          intervalString
      );
      ListenableFuture<Iterable<ImmutableSegmentLoadInfo>> segments = FutureUtils.transform(
          client.asyncRequest(
              new RequestBuilder(HttpMethod.GET, path),
              new BytesFullResponseHandler()
          ),
          holder -> JacksonUtils.readValue(
              jsonMapper,
              holder.getContent(),
              new TypeReference<Iterable<ImmutableSegmentLoadInfo>>()
              {
              }
          )
      );
      FutureUtils.getUnchecked(segments, true).forEach(retVal::add);
    }

    return retVal;
  }

  @Override
  public ListenableFuture<List<DataSegment>> fetchUsedSegments(String dataSource, List<Interval> intervals)
  {
    final String path = StringUtils.format(
        "/druid/coordinator/v1/metadata/datasources/%s/segments?full",
        StringUtils.urlEncode(dataSource)
    );

    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.POST, path)
                .jsonContent(jsonMapper, intervals),
            new BytesFullResponseHandler()
        ),
        holder -> JacksonUtils.readValue(jsonMapper, holder.getContent(), new TypeReference<List<DataSegment>>() {})
    );
  }

  @Override
  public ListenableFuture<List<DataSourceInformation>> fetchDataSourceInformation(Set<String> dataSources)
  {
    final String path = "/druid/coordinator/v1/metadata/dataSourceInformation";
    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.POST, path)
                .jsonContent(jsonMapper, dataSources),
            new BytesFullResponseHandler()
        ),
        holder -> JacksonUtils.readValue(jsonMapper, holder.getContent(), new TypeReference<List<DataSourceInformation>>() {})
    );
  }

  @Override
  public CoordinatorClientImpl withRetryPolicy(ServiceRetryPolicy retryPolicy)
  {
    return new CoordinatorClientImpl(client.withRetryPolicy(retryPolicy), jsonMapper);
  }
}
