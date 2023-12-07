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

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.client.ImmutableSegmentLoadInfo;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.rpc.MockServiceClient;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.metadata.DataSourceInformation;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class CoordinatorClientImplTest
{
  private ObjectMapper jsonMapper;
  private MockServiceClient serviceClient;
  private CoordinatorClient coordinatorClient;

  @Before
  public void setup()
  {
    jsonMapper = new DefaultObjectMapper();
    jsonMapper.setInjectableValues(
        new InjectableValues.Std(ImmutableMap.of(
            DataSegment.PruneSpecsHolder.class.getName(),
            DataSegment.PruneSpecsHolder.DEFAULT)));
    serviceClient = new MockServiceClient();
    coordinatorClient = new CoordinatorClientImpl(serviceClient, jsonMapper);
  }

  @After
  public void tearDown()
  {
    serviceClient.verify();
  }

  @Test
  public void test_isHandoffComplete() throws Exception
  {
    serviceClient.expectAndRespond(
        new RequestBuilder(
            HttpMethod.GET,
            "/druid/coordinator/v1/datasources/xyz/handoffComplete?"
            + "interval=2000-01-01T00%3A00%3A00.000Z%2F3000-01-01T00%3A00%3A00.000Z&"
            + "partitionNumber=2&"
            + "version=1"
        ),
        HttpResponseStatus.OK,
        ImmutableMap.of(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON),
        StringUtils.toUtf8("true")
    );

    Assert.assertEquals(
        true,
        coordinatorClient.isHandoffComplete(
            "xyz",
            new SegmentDescriptor(Intervals.of("2000/3000"), "1", 2)
        ).get()
    );
  }

  @Test
  public void test_fetchUsedSegment() throws Exception
  {
    final DataSegment segment =
        DataSegment.builder()
                   .dataSource("xyz")
                   .interval(Intervals.of("2000/3000"))
                   .version("1")
                   .shardSpec(new NumberedShardSpec(0, 1))
                   .size(1)
                   .build();

    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/druid/coordinator/v1/metadata/datasources/xyz/segments/def?includeUnused=false"),
        HttpResponseStatus.OK,
        ImmutableMap.of(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON),
        jsonMapper.writeValueAsBytes(segment)
    );

    Assert.assertEquals(
        segment,
        coordinatorClient.fetchSegment("xyz", "def", false).get()
    );
  }

  @Test
  public void test_fetchSegment() throws Exception
  {
    final DataSegment segment =
        DataSegment.builder()
                   .dataSource("xyz")
                   .interval(Intervals.of("2000/3000"))
                   .version("1")
                   .shardSpec(new NumberedShardSpec(0, 1))
                   .size(1)
                   .build();

    serviceClient.expectAndRespond(
        new RequestBuilder(
            HttpMethod.GET,
            "/druid/coordinator/v1/metadata/datasources/xyz/segments/def?includeUnused=true"
        ),
        HttpResponseStatus.OK,
        ImmutableMap.of(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON),
        jsonMapper.writeValueAsBytes(segment)
    );

    Assert.assertEquals(
        segment,
        coordinatorClient.fetchSegment("xyz", "def", true).get()
    );
  }

  @Test
  public void test_fetchUsedSegments() throws Exception
  {
    final List<Interval> intervals = Collections.singletonList(Intervals.of("2000/3000"));
    final DataSegment segment =
        DataSegment.builder()
                   .dataSource("xyz")
                   .interval(intervals.get(0))
                   .version("1")
                   .shardSpec(new NumberedShardSpec(0, 1))
                   .size(1)
                   .build();

    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.POST, "/druid/coordinator/v1/metadata/datasources/xyz/segments?full")
            .jsonContent(jsonMapper, intervals),
        HttpResponseStatus.OK,
        ImmutableMap.of(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON),
        jsonMapper.writeValueAsBytes(Collections.singletonList(segment))
    );

    Assert.assertEquals(
        Collections.singletonList(segment),
        coordinatorClient.fetchUsedSegments("xyz", intervals).get()
    );
  }

  @Test
  public void test_fetchDataSourceInformation() throws Exception
  {
    String foo = "foo";

    DataSourceInformation fooInfo = new DataSourceInformation(
        "foo",
        RowSignature.builder()
                    .add("d1", ColumnType.FLOAT)
                    .add("d2", ColumnType.DOUBLE)
                    .build()
    );

    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.POST, "/druid/coordinator/v1/metadata/dataSourceInformation")
            .jsonContent(jsonMapper, Collections.singletonList(foo)),
        HttpResponseStatus.OK,
        ImmutableMap.of(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON),
        jsonMapper.writeValueAsBytes(Collections.singletonList(fooInfo))
    );

    Assert.assertEquals(
        Collections.singletonList(fooInfo),
        coordinatorClient.fetchDataSourceInformation(Collections.singleton(foo)).get()
    );
  }

  @Test
  public void test_fetchServerViewSegments() throws Exception
  {

    final List<Interval> intervals = ImmutableList.of(
        Intervals.of("2001/2002"),
        Intervals.of("2501/2502")
    );

    final Set<DruidServerMetadata> serverMetadataSet =
        ImmutableSet.of(
            new DruidServerMetadata(
                "TEST_SERVER",
                "testhost:9092",
                null,
                1,
                ServerType.INDEXER_EXECUTOR,
                "tier1",
                0
            )
        );

    final ImmutableSegmentLoadInfo immutableSegmentLoadInfo1 = new ImmutableSegmentLoadInfo(
        DataSegment.builder()
                   .dataSource("xyz")
                   .interval(intervals.get(0))
                   .version("1")
                   .shardSpec(new NumberedShardSpec(0, 1))
                   .size(1)
                   .build(),
        serverMetadataSet
        );

    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/druid/coordinator/v1/datasources/xyz/intervals/2001-01-01T00:00:00.000Z_2002-01-01T00:00:00.000Z/serverview?full"),
        HttpResponseStatus.OK,
        ImmutableMap.of(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON),
        jsonMapper.writeValueAsBytes(Collections.singletonList(immutableSegmentLoadInfo1))
    );

    final ImmutableSegmentLoadInfo immutableSegmentLoadInfo2 = new ImmutableSegmentLoadInfo(
        DataSegment.builder()
                   .dataSource("xyz")
                   .interval(intervals.get(1))
                   .version("1")
                   .shardSpec(new NumberedShardSpec(0, 1))
                   .size(1)
                   .build(),
        serverMetadataSet
    );

    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/druid/coordinator/v1/datasources/xyz/intervals/2501-01-01T00:00:00.000Z_2502-01-01T00:00:00.000Z/serverview?full"),
        HttpResponseStatus.OK,
        ImmutableMap.of(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON),
        jsonMapper.writeValueAsBytes(Collections.singletonList(immutableSegmentLoadInfo2))
    );

    List<ImmutableSegmentLoadInfo> segmentLoadInfoList =
        ImmutableList.of(immutableSegmentLoadInfo1, immutableSegmentLoadInfo2);

    Assert.assertEquals(
        segmentLoadInfoList,
        coordinatorClient.fetchServerViewSegments("xyz", intervals)
    );
  }
}
