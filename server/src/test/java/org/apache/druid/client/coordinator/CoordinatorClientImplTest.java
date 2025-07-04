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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Injector;
import org.apache.druid.client.BootstrapSegmentsResponse;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ImmutableSegmentLoadInfo;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.guice.StartupInjectorBuilder;
import org.apache.druid.initialization.CoreInjectorBuilder;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.lookup.LookupExtractorFactory;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainer;
import org.apache.druid.query.lookup.MapLookupExtractorFactory;
import org.apache.druid.rpc.HttpResponseException;
import org.apache.druid.rpc.MockServiceClient;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.metadata.DataSourceInformation;
import org.apache.druid.server.compaction.CompactionStatusResponse;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.rules.IntervalLoadRule;
import org.apache.druid.server.coordinator.rules.Rule;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.PruneLoadSpec;
import org.apache.druid.timeline.SegmentStatusInCluster;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class CoordinatorClientImplTest
{
  private ObjectMapper jsonMapper;
  private MockServiceClient serviceClient;
  private CoordinatorClient coordinatorClient;

  private static final DataSegment SEGMENT1 = DataSegment.builder()
                                                         .dataSource("xyz")
                                                         .interval(Intervals.of("1000/2000"))
                                                         .version("1")
                                                         .loadSpec(ImmutableMap.of("type", "local", "loc", "foo"))
                                                         .shardSpec(new NumberedShardSpec(0, 1))
                                                         .size(1)
                                                         .build();

  private static final DataSegment SEGMENT2 = DataSegment.builder()
                                                         .dataSource("xyz")
                                                         .interval(Intervals.of("2000/3000"))
                                                         .version("1")
                                                         .loadSpec(ImmutableMap.of("type", "local", "loc", "bar"))
                                                         .shardSpec(new NumberedShardSpec(0, 1))
                                                         .size(1)
                                                         .build();

  private static final DataSegment SEGMENT3 = DataSegment.builder()
                                                         .dataSource("abc")
                                                         .interval(Intervals.of("2000/3000"))
                                                         .version("1")
                                                         .loadSpec(ImmutableMap.of("type", "local", "loc", "bar"))
                                                         .shardSpec(new NumberedShardSpec(0, 1))
                                                         .size(1)
                                                         .build();

  @Before
  public void setup()
  {
    jsonMapper = new DefaultObjectMapper();
    jsonMapper.setInjectableValues(
        new InjectableValues.Std(ImmutableMap.of(
            DataSegment.PruneSpecsHolder.class.getName(),
            DataSegment.PruneSpecsHolder.DEFAULT
        ))
    );
    jsonMapper.registerSubtypes(MapLookupExtractorFactory.class);
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
        new RequestBuilder(
            HttpMethod.GET,
            "/druid/coordinator/v1/metadata/datasources/xyz/segments/def?includeUnused=false"
        ),
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
  public void test_fetchBootstrapSegments() throws Exception
  {
    final List<DataSegment> expectedSegments = ImmutableList.of(SEGMENT1, SEGMENT2);

    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.POST, "/druid/coordinator/v1/metadata/bootstrapSegments"),
        HttpResponseStatus.OK,
        ImmutableMap.of(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON),
        jsonMapper.writeValueAsBytes(expectedSegments)
    );

    final ListenableFuture<BootstrapSegmentsResponse> response = coordinatorClient.fetchBootstrapSegments();
    Assert.assertNotNull(response);

    final ImmutableList<DataSegment> observedDataSegments = ImmutableList.copyOf(response.get().getIterator());
    for (int idx = 0; idx < expectedSegments.size(); idx++) {
      Assert.assertEquals(expectedSegments.get(idx).getLoadSpec(), observedDataSegments.get(idx).getLoadSpec());
    }
  }

  /**
   * Set up a Guice injector with PruneLoadSpec set to true. This test verifies that the bootstrap segments API
   * always return segments with load specs present, ensuring they can be loaded anywhere.
   */
  @Test
  public void test_fetchBootstrapSegmentsAreLoadableWhenPruneLoadSpecIsEnabled() throws Exception
  {
    final List<DataSegment> expectedSegments = ImmutableList.of(SEGMENT1, SEGMENT2);

    // Set up a coordinator client with PruneLoadSpec set to true in the injector
    final Injector injector = new CoreInjectorBuilder(new StartupInjectorBuilder().build())
        .addModule(binder -> binder.bindConstant().annotatedWith(PruneLoadSpec.class).to(true))
        .build();

    final ObjectMapper objectMapper = injector.getInstance(ObjectMapper.class);
    final CoordinatorClient coordinatorClient = new CoordinatorClientImpl(serviceClient, objectMapper);

    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.POST, "/druid/coordinator/v1/metadata/bootstrapSegments"),
        HttpResponseStatus.OK,
        ImmutableMap.of(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON),
        objectMapper.writeValueAsBytes(expectedSegments)
    );

    final ListenableFuture<BootstrapSegmentsResponse> response = coordinatorClient.fetchBootstrapSegments();
    Assert.assertNotNull(response);

    final ImmutableList<DataSegment> observedDataSegments = ImmutableList.copyOf(response.get().getIterator());
    Assert.assertEquals(expectedSegments, observedDataSegments);
    for (int idx = 0; idx < expectedSegments.size(); idx++) {
      Assert.assertEquals(expectedSegments.get(idx).getLoadSpec(), observedDataSegments.get(idx).getLoadSpec());
    }
  }

  @Test
  public void test_fetchEmptyBootstrapSegments() throws Exception
  {
    final List<DataSegment> segments = ImmutableList.of();

    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.POST, "/druid/coordinator/v1/metadata/bootstrapSegments"),
        HttpResponseStatus.OK,
        ImmutableMap.of(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON),
        jsonMapper.writeValueAsBytes(segments)
    );

    final ListenableFuture<BootstrapSegmentsResponse> response = coordinatorClient.fetchBootstrapSegments();
    Assert.assertNotNull(response);

    Assert.assertEquals(
        segments,
        ImmutableList.copyOf(response.get().getIterator())
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
        new RequestBuilder(
            HttpMethod.GET,
            "/druid/coordinator/v1/datasources/xyz/intervals/2001-01-01T00:00:00.000Z_2002-01-01T00:00:00.000Z/serverview?full"
        ),
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
        new RequestBuilder(
            HttpMethod.GET,
            "/druid/coordinator/v1/datasources/xyz/intervals/2501-01-01T00:00:00.000Z_2502-01-01T00:00:00.000Z/serverview?full"
        ),
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

  @Test
  public void test_getCompactionSnapshots_nullDataSource()
      throws JsonProcessingException, ExecutionException, InterruptedException
  {
    final List<AutoCompactionSnapshot> compactionSnapshots = List.of(
        AutoCompactionSnapshot.builder("ds1")
                              .withStatus(AutoCompactionSnapshot.ScheduleStatus.RUNNING)
                              .build(),
        AutoCompactionSnapshot.builder("ds2")
                              .withStatus(AutoCompactionSnapshot.ScheduleStatus.NOT_ENABLED)
                              .build()
    );
    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/druid/coordinator/v1/compaction/status"),
        HttpResponseStatus.OK,
        Map.of(),
        DefaultObjectMapper.INSTANCE.writeValueAsBytes(new CompactionStatusResponse(compactionSnapshots))
    );

    Assert.assertEquals(
        new CompactionStatusResponse(compactionSnapshots),
        coordinatorClient.getCompactionSnapshots(null).get()
    );
  }

  @Test
  public void test_getCompactionSnapshots_nonNullDataSource() throws Exception
  {
    final List<AutoCompactionSnapshot> compactionSnapshots = List.of(
        AutoCompactionSnapshot.builder("ds1").build()
    );
    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/druid/coordinator/v1/compaction/status?datasources=ds1"),
        HttpResponseStatus.OK,
        Map.of(),
        DefaultObjectMapper.INSTANCE.writeValueAsBytes(new CompactionStatusResponse(compactionSnapshots))
    );

    Assert.assertEquals(
        new CompactionStatusResponse(compactionSnapshots),
        coordinatorClient.getCompactionSnapshots("ds1").get()
    );
  }

  @Test
  public void test_getCoordinatorDynamicConfig() throws Exception
  {
    CoordinatorDynamicConfig config = CoordinatorDynamicConfig
        .builder()
        .withMaxSegmentsToMove(105)
        .withReplicantLifetime(500)
        .withReplicationThrottleLimit(5)
        .build();

    serviceClient.expectAndRespond(
        new RequestBuilder(
            HttpMethod.GET,
            "/druid/coordinator/v1/config"
        ),
        HttpResponseStatus.OK,
        ImmutableMap.of(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON),
        DefaultObjectMapper.INSTANCE.writeValueAsBytes(config)
    );

    Assert.assertEquals(
        config,
        coordinatorClient.getCoordinatorDynamicConfig().get()
    );
  }

  @Test
  public void test_updateCoordinatorDynamicConfig() throws Exception
  {
    final CoordinatorDynamicConfig config = CoordinatorDynamicConfig
        .builder()
        .withMaxSegmentsToMove(105)
        .withReplicantLifetime(500)
        .withReplicationThrottleLimit(5)
        .build();

    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.POST, "/druid/coordinator/v1/config")
            .jsonContent(jsonMapper, config),
        HttpResponseStatus.OK,
        ImmutableMap.of(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON),
        DefaultObjectMapper.INSTANCE.writeValueAsBytes(null)
    );

    Assert.assertNull(coordinatorClient.updateCoordinatorDynamicConfig(config).get());
  }

  @Test
  public void test_updateAllLookups_withEmptyLookup() throws Exception
  {
    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.POST, "/druid/coordinator/v1/lookups/config")
            .jsonContent(jsonMapper, Map.of()),
        HttpResponseStatus.OK,
        Map.of(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON),
        DefaultObjectMapper.INSTANCE.writeValueAsBytes(null)
    );

    Assert.assertNull(coordinatorClient.updateAllLookups(Map.of()).get());
  }

  @Test
  public void test_fetchLookupsForTierSync_detailedEnabled() throws Exception
  {
    LookupExtractorFactory lookupData = new MapLookupExtractorFactory(
        Map.of(
            "77483", "United States",
            "77484", "India"
        ),
        true
    );
    LookupExtractorFactoryContainer lookupDataContainer = new LookupExtractorFactoryContainer("v0", lookupData);
    Map<String, LookupExtractorFactoryContainer> lookups = Map.of(
        "default_tier", lookupDataContainer
    );

    serviceClient.expectAndRespond(
        new RequestBuilder(
            HttpMethod.GET,
            "/druid/coordinator/v1/lookups/config/default_tier?detailed=true"
        ),
        HttpResponseStatus.OK,
        Map.of(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON),
        DefaultObjectMapper.INSTANCE.writeValueAsBytes(lookups)
    );

    Assert.assertEquals(
        lookups,
        coordinatorClient.fetchLookupsForTierSync("default_tier")
    );
  }

  @Test
  public void test_getMetadataSegments() throws JsonProcessingException
  {
    final List<DataSegment> segments = ImmutableList.of(SEGMENT1, SEGMENT2, SEGMENT3);

    serviceClient.expectAndRespond(
        new RequestBuilder(
            HttpMethod.GET,
            "/druid/coordinator/v1/metadata/segments?includeOvershadowedStatus&includeRealtimeSegments"
        ),
        HttpResponseStatus.OK,
        ImmutableMap.of(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON),
        jsonMapper.writeValueAsBytes(segments)
    );

    CloseableIterator<SegmentStatusInCluster> iterator = FutureUtils.getUnchecked(
        coordinatorClient.getMetadataSegments(null),
        true
    );
    List<SegmentStatusInCluster> actualSegments = new ArrayList<>();
    while (iterator.hasNext()) {
      actualSegments.add(iterator.next());
    }
    Assert.assertEquals(
        segments,
        actualSegments.stream()
                      .map(SegmentStatusInCluster::getDataSegment)
                      .collect(ImmutableList.toImmutableList())
    );
  }

  @Test
  public void test_getMetadataSegments_filterByDataSource() throws Exception
  {
    serviceClient.expectAndRespond(
        new RequestBuilder(
            HttpMethod.GET,
            "/druid/coordinator/v1/metadata/segments?includeOvershadowedStatus&includeRealtimeSegments&dataSource=abc"
        ),
        HttpResponseStatus.OK,
        ImmutableMap.of(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON),
        jsonMapper.writeValueAsBytes(ImmutableList.of(SEGMENT3))
    );

    CloseableIterator<SegmentStatusInCluster> iterator = FutureUtils.getUnchecked(
        coordinatorClient.getMetadataSegments(
            Collections.singleton("abc")
        ), true
    );

    List<SegmentStatusInCluster> actualSegments = new ArrayList<>();
    while (iterator.hasNext()) {
      actualSegments.add(iterator.next());
    }
    Assert.assertEquals(
        ImmutableList.of(SEGMENT3),
        actualSegments.stream()
                      .map(SegmentStatusInCluster::getDataSegment)
                      .collect(ImmutableList.toImmutableList())
    );
  }

  @Test
  public void test_getRules() throws Exception
  {
    final Map<String, List<Rule>> rules = ImmutableMap.of(
        "xyz", ImmutableList.of(
            new IntervalLoadRule(
                Intervals.of("2025-01-01/2025-02-01"),
                ImmutableMap.of(DruidServer.DEFAULT_TIER, DruidServer.DEFAULT_NUM_REPLICANTS),
                null
            ),
            new IntervalLoadRule(
                Intervals.of("2025-02-01/2025-03-01"),
                ImmutableMap.of(DruidServer.DEFAULT_TIER, DruidServer.DEFAULT_NUM_REPLICANTS),
                null
            )
        )
    );

    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/druid/coordinator/v1/rules"),
        HttpResponseStatus.OK,
        ImmutableMap.of(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON),
        jsonMapper.writeValueAsBytes(rules)
    );

    Assert.assertEquals(
        rules,
        coordinatorClient.getRules().get()
    );
  }

  @Test
  public void test_getRules_HttpException_throwsError()
  {
    serviceClient.expectAndThrow(
        new RequestBuilder(HttpMethod.GET, "/druid/coordinator/v1/rules"),
        new HttpResponseException(
            new StringFullResponseHolder(
                new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND),
                StandardCharsets.UTF_8
            )
        )
    );

    RuntimeException thrown = Assert.assertThrows(
        RuntimeException.class,
        () -> FutureUtils.getUnchecked(coordinatorClient.getRules(), true)
    );
    Assert.assertTrue(Throwables.getRootCause(thrown) instanceof HttpResponseException);
  }

  @Test
  public void test_findCurrentLeader() throws Exception
  {
    String leaderUrl = "http://localhost:8081";
    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/druid/coordinator/v1/leader"),
        HttpResponseStatus.OK,
        ImmutableMap.of(HttpHeaders.CONTENT_TYPE, MediaType.TEXT_PLAIN),
        StringUtils.toUtf8(leaderUrl)
    );

    Assert.assertEquals(
        leaderUrl,
        FutureUtils.getUnchecked(coordinatorClient.findCurrentLeader(), true).toString()
    );
  }

  @Test
  public void test_findCurrentLeader_invalidUrl()
  {
    String invalidLeaderUrl = "{{1234invalidUrl";
    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/druid/coordinator/v1/leader"),
        HttpResponseStatus.OK,
        ImmutableMap.of(HttpHeaders.CONTENT_TYPE, MediaType.TEXT_PLAIN),
        StringUtils.toUtf8(invalidLeaderUrl)
    );
    Assert.assertThrows(
        RuntimeException.class,
        () -> FutureUtils.getUnchecked(coordinatorClient.findCurrentLeader(), true)
    );
  }

  @Test
  public void test_findCurrentLeader_runtimeException()
  {
    serviceClient.expectAndThrow(
        new RequestBuilder(HttpMethod.GET, "/druid/coordinator/v1/leader"),
        new RuntimeException("Simulated runtime error")
    );

    Assert.assertThrows(
        RuntimeException.class,
        () -> FutureUtils.getUnchecked(coordinatorClient.findCurrentLeader(), true)
    );
  }

  @Test
  public void test_findCurrentLeader_httpResponseException()
  {
    serviceClient.expectAndThrow(
        new RequestBuilder(HttpMethod.GET, "/druid/coordinator/v1/leader"),
        new HttpResponseException(
            new StringFullResponseHolder(
                new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND),
                StandardCharsets.UTF_8
            )
        )
    );
    // try and assert that the root cause is an HttpResponseException
    try {
      FutureUtils.getUnchecked(coordinatorClient.findCurrentLeader(), true);
    }
    catch (Exception e) {
      Throwable throwable = Throwables.getRootCause(e);
      Assert.assertTrue(throwable instanceof HttpResponseException);
    }
  }

  @Test
  public void test_postLoadRules() throws Exception
  {
    final List<Rule> rules = ImmutableList.of(
        new IntervalLoadRule(
            Intervals.of("2025-01-01/2025-02-01"),
            ImmutableMap.of(DruidServer.DEFAULT_TIER, DruidServer.DEFAULT_NUM_REPLICANTS),
            null
        )
    );

    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.POST, "/druid/coordinator/v1/rules/xyz")
            .jsonContent(jsonMapper, rules),
        HttpResponseStatus.OK,
        ImmutableMap.of(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON),
        jsonMapper.writeValueAsBytes(null)
    );

    Assert.assertNull(coordinatorClient.postLoadRules("xyz", rules).get());
  }
}
