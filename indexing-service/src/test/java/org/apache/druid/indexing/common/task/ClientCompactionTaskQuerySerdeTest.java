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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.AnnotationIntrospectorPair;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.client.coordinator.NoopCoordinatorClient;
import org.apache.druid.client.indexing.ClientCompactionIOConfig;
import org.apache.druid.client.indexing.ClientCompactionIntervalSpec;
import org.apache.druid.client.indexing.ClientCompactionTaskDimensionsSpec;
import org.apache.druid.client.indexing.ClientCompactionTaskGranularitySpec;
import org.apache.druid.client.indexing.ClientCompactionTaskQuery;
import org.apache.druid.client.indexing.ClientCompactionTaskQueryTuningConfig;
import org.apache.druid.client.indexing.ClientCompactionTaskTransformSpec;
import org.apache.druid.client.indexing.ClientTaskQuery;
import org.apache.druid.client.indexing.NoopOverlordClient;
import org.apache.druid.data.input.SegmentsSplitHintSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.guice.GuiceAnnotationIntrospector;
import org.apache.druid.guice.GuiceInjectableValues;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexing.common.RetryPolicyConfig;
import org.apache.druid.indexing.common.RetryPolicyFactory;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.data.CompressionFactory.LongEncodingStrategy;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import org.apache.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

public class ClientCompactionTaskQuerySerdeTest
{
  private static final RowIngestionMetersFactory ROW_INGESTION_METERS_FACTORY =
      new TestUtils().getRowIngestionMetersFactory();
  private static final CoordinatorClient COORDINATOR_CLIENT = new NoopCoordinatorClient();
  private static final AppenderatorsManager APPENDERATORS_MANAGER = new TestAppenderatorsManager();

  @Test
  public void testClientCompactionTaskQueryToCompactionTask() throws IOException
  {
    final ObjectMapper mapper = setupInjectablesInObjectMapper(new DefaultObjectMapper());
    final ClientCompactionTaskQuery query = new ClientCompactionTaskQuery(
        "id",
        "datasource",
        new ClientCompactionIOConfig(
            new ClientCompactionIntervalSpec(
                Intervals.of("2019/2020"),
                "testSha256OfSortedSegmentIds"
            ),
            true
        ),
        new ClientCompactionTaskQueryTuningConfig(
            null,
            null,
            40000,
            2000L,
            null,
            new SegmentsSplitHintSpec(new HumanReadableBytes(100000L), 10),
            new DynamicPartitionsSpec(100, 30000L),
            IndexSpec.builder()
                     .withDimensionCompression(CompressionStrategy.LZ4)
                     .withMetricCompression(CompressionStrategy.LZF)
                     .withLongEncoding(LongEncodingStrategy.LONGS)
                     .build(),
            IndexSpec.builder()
                     .withDimensionCompression(CompressionStrategy.LZ4)
                     .withMetricCompression(CompressionStrategy.UNCOMPRESSED)
                     .withLongEncoding(LongEncodingStrategy.AUTO)
                     .build(),
            2,
            1000L,
            TmpFileSegmentWriteOutMediumFactory.instance(),
            100,
            5,
            1000L,
            new Duration(3000L),
            7,
            1000,
            100,
            2
        ),
        new ClientCompactionTaskGranularitySpec(Granularities.DAY, Granularities.HOUR, true),
        new ClientCompactionTaskDimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("ts", "dim"))),
        new AggregatorFactory[] {new CountAggregatorFactory("cnt")},
        new ClientCompactionTaskTransformSpec(new SelectorDimFilter("dim1", "foo", null)),
        ImmutableMap.of("key", "value")
    );

    final byte[] json = mapper.writeValueAsBytes(query);
    final CompactionTask task = (CompactionTask) mapper.readValue(json, Task.class);

    Assert.assertEquals(query.getId(), task.getId());
    Assert.assertEquals(query.getDataSource(), task.getDataSource());
    Assert.assertTrue(task.getIoConfig().getInputSpec() instanceof CompactionIntervalSpec);
    Assert.assertEquals(
        query.getIoConfig().getInputSpec().getInterval(),
        ((CompactionIntervalSpec) task.getIoConfig().getInputSpec()).getInterval()
    );
    Assert.assertEquals(
        query.getIoConfig().getInputSpec().getSha256OfSortedSegmentIds(),
        ((CompactionIntervalSpec) task.getIoConfig().getInputSpec()).getSha256OfSortedSegmentIds()
    );
    Assert.assertEquals(
        query.getTuningConfig().getMaxRowsInMemory().intValue(),
        task.getTuningConfig().getMaxRowsInMemory()
    );
    Assert.assertEquals(
        query.getTuningConfig().getMaxBytesInMemory().longValue(),
        task.getTuningConfig().getMaxBytesInMemory()
    );
    Assert.assertEquals(
        query.getTuningConfig().getSplitHintSpec(),
        task.getTuningConfig().getSplitHintSpec()
    );
    Assert.assertEquals(
        query.getTuningConfig().getPartitionsSpec(),
        task.getTuningConfig().getPartitionsSpec()
    );
    Assert.assertEquals(
        query.getTuningConfig().getIndexSpec(),
        task.getTuningConfig().getIndexSpec()
    );
    Assert.assertEquals(
        query.getTuningConfig().getIndexSpecForIntermediatePersists(),
        task.getTuningConfig().getIndexSpecForIntermediatePersists()
    );
    Assert.assertEquals(
        query.getTuningConfig().getPushTimeout().longValue(),
        task.getTuningConfig().getPushTimeout()
    );
    Assert.assertEquals(
        query.getTuningConfig().getSegmentWriteOutMediumFactory(),
        task.getTuningConfig().getSegmentWriteOutMediumFactory()
    );
    Assert.assertEquals(
        query.getTuningConfig().getMaxNumConcurrentSubTasks().intValue(),
        task.getTuningConfig().getMaxNumConcurrentSubTasks()
    );
    Assert.assertEquals(
        query.getTuningConfig().getMaxRetry().intValue(),
        task.getTuningConfig().getMaxRetry()
    );
    Assert.assertEquals(
        query.getTuningConfig().getTaskStatusCheckPeriodMs().longValue(),
        task.getTuningConfig().getTaskStatusCheckPeriodMs()
    );
    Assert.assertEquals(
        query.getTuningConfig().getChatHandlerTimeout(),
        task.getTuningConfig().getChatHandlerTimeout()
    );
    Assert.assertEquals(
        query.getTuningConfig().getMaxNumSegmentsToMerge().intValue(),
        task.getTuningConfig().getMaxNumSegmentsToMerge()
    );
    Assert.assertEquals(
        query.getTuningConfig().getTotalNumMergeTasks().intValue(),
        task.getTuningConfig().getTotalNumMergeTasks()
    );
    Assert.assertEquals(
        query.getGranularitySpec(),
        task.getGranularitySpec()
    );
    Assert.assertEquals(
        query.getGranularitySpec().getQueryGranularity(),
        task.getGranularitySpec().getQueryGranularity()
    );
    Assert.assertEquals(
        query.getGranularitySpec().getSegmentGranularity(),
        task.getGranularitySpec().getSegmentGranularity()
    );
    Assert.assertEquals(
        query.getGranularitySpec().isRollup(),
        task.getGranularitySpec().isRollup()
    );
    Assert.assertEquals(
        query.getIoConfig().isDropExisting(),
        task.getIoConfig().isDropExisting()
    );
    Assert.assertEquals(query.getContext(), task.getContext());
    Assert.assertEquals(
        query.getDimensionsSpec().getDimensions(),
        task.getDimensionsSpec().getDimensions()
    );
    Assert.assertEquals(
        query.getTransformSpec().getFilter(),
        task.getTransformSpec().getFilter()
    );
    Assert.assertArrayEquals(
        query.getMetricsSpec(),
        task.getMetricsSpec()
    );
  }

  @Test
  public void testCompactionTaskToClientCompactionTaskQuery() throws IOException
  {
    final ObjectMapper mapper = setupInjectablesInObjectMapper(new DefaultObjectMapper());
    final CompactionTask.Builder builder = new CompactionTask.Builder(
        "datasource",
        new SegmentCacheManagerFactory(mapper),
        new RetryPolicyFactory(new RetryPolicyConfig())
    );
    final CompactionTask task = builder
        .inputSpec(new CompactionIntervalSpec(Intervals.of("2019/2020"), "testSha256OfSortedSegmentIds"), true)
        .tuningConfig(
            new ParallelIndexTuningConfig(
                null,
                null,
                new OnheapIncrementalIndex.Spec(true),
                40000,
                2000L,
                null,
                null,
                null,
                new SegmentsSplitHintSpec(new HumanReadableBytes(100000L), 10),
                new DynamicPartitionsSpec(100, 30000L),
                IndexSpec.builder()
                         .withDimensionCompression(CompressionStrategy.LZ4)
                         .withMetricCompression(CompressionStrategy.LZF)
                         .withLongEncoding(LongEncodingStrategy.LONGS)
                         .build(),
                IndexSpec.builder()
                         .withDimensionCompression(CompressionStrategy.LZ4)
                         .withMetricCompression(CompressionStrategy.UNCOMPRESSED)
                         .withLongEncoding(LongEncodingStrategy.AUTO)
                         .build(),
                2,
                null,
                null,
                1000L,
                TmpFileSegmentWriteOutMediumFactory.instance(),
                null,
                100,
                5,
                1000L,
                new Duration(3000L),
                7,
                1000,
                100,
                null,
                null,
                null,
                2,
                null,
                null,
                null
            )
        )
        .granularitySpec(new ClientCompactionTaskGranularitySpec(Granularities.DAY, Granularities.HOUR, true))
        .dimensionsSpec(
            DimensionsSpec.builder()
                          .setDimensions(DimensionsSpec.getDefaultSchemas(ImmutableList.of("ts", "dim")))
                          .setDimensionExclusions(ImmutableList.of("__time", "val"))
                          .build()
        )
        .metricsSpec(new AggregatorFactory[] {new CountAggregatorFactory("cnt")})
        .transformSpec(new ClientCompactionTaskTransformSpec(new SelectorDimFilter("dim1", "foo", null)))
        .build();

    final ClientCompactionTaskQuery expected = new ClientCompactionTaskQuery(
        task.getId(),
        "datasource",
        new ClientCompactionIOConfig(
            new ClientCompactionIntervalSpec(
                Intervals.of("2019/2020"),
                "testSha256OfSortedSegmentIds"
            ),
            true
        ),
        new ClientCompactionTaskQueryTuningConfig(
            100,
            new OnheapIncrementalIndex.Spec(true),
            40000,
            2000L,
            30000L,
            new SegmentsSplitHintSpec(new HumanReadableBytes(100000L), 10),
            new DynamicPartitionsSpec(100, 30000L),
            IndexSpec.builder()
                     .withDimensionCompression(CompressionStrategy.LZ4)
                     .withMetricCompression(CompressionStrategy.LZF)
                     .withLongEncoding(LongEncodingStrategy.LONGS)
                     .build(),
            IndexSpec.builder()
                     .withDimensionCompression(CompressionStrategy.LZ4)
                     .withMetricCompression(CompressionStrategy.UNCOMPRESSED)
                     .withLongEncoding(LongEncodingStrategy.AUTO)
                     .build(),
            2,
            1000L,
            TmpFileSegmentWriteOutMediumFactory.instance(),
            100,
            5,
            1000L,
            new Duration(3000L),
            7,
            1000,
            100,
            2
        ),
        new ClientCompactionTaskGranularitySpec(Granularities.DAY, Granularities.HOUR, true),
        new ClientCompactionTaskDimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("ts", "dim"))),
        new AggregatorFactory[] {new CountAggregatorFactory("cnt")},
        new ClientCompactionTaskTransformSpec(new SelectorDimFilter("dim1", "foo", null)),
        new HashMap<>()
    );

    final byte[] json = mapper.writeValueAsBytes(task);
    final ClientCompactionTaskQuery actual = (ClientCompactionTaskQuery) mapper.readValue(json, ClientTaskQuery.class);

    Assert.assertEquals(expected, actual);
  }

  private static ObjectMapper setupInjectablesInObjectMapper(ObjectMapper objectMapper)
  {
    final GuiceAnnotationIntrospector guiceIntrospector = new GuiceAnnotationIntrospector();
    objectMapper.setAnnotationIntrospectors(
        new AnnotationIntrospectorPair(
            guiceIntrospector,
            objectMapper.getSerializationConfig().getAnnotationIntrospector()
        ),
        new AnnotationIntrospectorPair(
            guiceIntrospector,
            objectMapper.getDeserializationConfig().getAnnotationIntrospector()
        )
    );
    GuiceInjectableValues injectableValues = new GuiceInjectableValues(
        GuiceInjectors.makeStartupInjectorWithModules(
            ImmutableList.of(
                binder -> {
                  binder.bind(AuthorizerMapper.class).toInstance(AuthTestUtils.TEST_AUTHORIZER_MAPPER);
                  binder.bind(ChatHandlerProvider.class).toInstance(new NoopChatHandlerProvider());
                  binder.bind(RowIngestionMetersFactory.class).toInstance(ROW_INGESTION_METERS_FACTORY);
                  binder.bind(CoordinatorClient.class).toInstance(COORDINATOR_CLIENT);
                  binder.bind(SegmentCacheManagerFactory.class).toInstance(new SegmentCacheManagerFactory(objectMapper));
                  binder.bind(AppenderatorsManager.class).toInstance(APPENDERATORS_MANAGER);
                  binder.bind(OverlordClient.class).toInstance(new NoopOverlordClient());
                }
            )
        )
    );
    objectMapper.setInjectableValues(injectableValues);
    objectMapper.registerSubtypes(new NamedType(ParallelIndexTuningConfig.class, "index_parallel"));
    return objectMapper;
  }
}
