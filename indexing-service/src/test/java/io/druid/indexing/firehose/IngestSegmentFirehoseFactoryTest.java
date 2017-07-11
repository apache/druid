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

package io.druid.indexing.firehose;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.AnnotationIntrospectorPair;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Module;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.common.utils.JodaUtils;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.JSONParseSpec;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.SpatialDimensionSchema;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.guice.GuiceAnnotationIntrospector;
import io.druid.guice.GuiceInjectableValues;
import io.druid.guice.GuiceInjectors;
import io.druid.indexing.common.SegmentLoaderFactory;
import io.druid.indexing.common.TaskToolboxFactory;
import io.druid.indexing.common.TestUtils;
import io.druid.indexing.common.actions.LocalTaskActionClientFactory;
import io.druid.indexing.common.actions.TaskActionToolbox;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.indexing.common.config.TaskStorageConfig;
import io.druid.indexing.overlord.HeapMemoryTaskStorage;
import io.druid.indexing.overlord.TaskLockbox;
import io.druid.indexing.overlord.supervisor.SupervisorManager;
import io.druid.java.util.common.IOE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.IndexSpec;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.loading.DataSegmentArchiver;
import io.druid.segment.loading.DataSegmentKiller;
import io.druid.segment.loading.DataSegmentMover;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.loading.LocalDataSegmentPuller;
import io.druid.segment.loading.LocalLoadSpec;
import io.druid.segment.loading.SegmentLoaderConfig;
import io.druid.segment.loading.SegmentLoaderLocalCacheManager;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.segment.loading.StorageLocationConfig;
import io.druid.segment.realtime.firehose.IngestSegmentFirehose;
import io.druid.segment.realtime.plumber.SegmentHandoffNotifierFactory;
import io.druid.server.metrics.NoopServiceEmitter;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NumberedShardSpec;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
@RunWith(Parameterized.class)
public class IngestSegmentFirehoseFactoryTest
{
  private static final ObjectMapper MAPPER;
  private static final IndexMergerV9 INDEX_MERGER_V9;
  private static final IndexIO INDEX_IO;

  static {
    TestUtils testUtils = new TestUtils();
    MAPPER = setupInjectablesInObjectMapper(testUtils.getTestObjectMapper());
    INDEX_MERGER_V9 = testUtils.getTestIndexMergerV9();
    INDEX_IO = testUtils.getTestIndexIO();
  }

  @Parameterized.Parameters(name = "{1}")
  public static Collection<Object[]> constructorFeeder() throws IOException
  {
    final IndexSpec indexSpec = new IndexSpec();

    final HeapMemoryTaskStorage ts = new HeapMemoryTaskStorage(
        new TaskStorageConfig(null)
        {
        }
    );
    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(JodaUtils.MIN_INSTANT)
        .withDimensionsSpec(ROW_PARSER)
        .withMetrics(
            new LongSumAggregatorFactory(METRIC_LONG_NAME, DIM_LONG_NAME),
            new DoubleSumAggregatorFactory(METRIC_FLOAT_NAME, DIM_FLOAT_NAME)
        )
        .build();
    final IncrementalIndex index = new IncrementalIndex.Builder()
        .setIndexSchema(schema)
        .setMaxRowCount(MAX_ROWS * MAX_SHARD_NUMBER)
        .buildOnheap();

    for (Integer i = 0; i < MAX_ROWS; ++i) {
      index.add(ROW_PARSER.parse(buildRow(i.longValue())));
    }

    if (!persistDir.mkdirs() && !persistDir.exists()) {
      throw new IOE("Could not create directory at [%s]", persistDir.getAbsolutePath());
    }
    INDEX_MERGER_V9.persist(index, persistDir, indexSpec);

    final TaskLockbox tl = new TaskLockbox(ts, 300);
    final IndexerSQLMetadataStorageCoordinator mdc = new IndexerSQLMetadataStorageCoordinator(null, null, null)
    {
      final private Set<DataSegment> published = Sets.newHashSet();
      final private Set<DataSegment> nuked = Sets.newHashSet();

      @Override
      public List<DataSegment> getUsedSegmentsForInterval(String dataSource, Interval interval) throws IOException
      {
        return ImmutableList.copyOf(segmentSet);
      }

      @Override
      public List<DataSegment> getUsedSegmentsForIntervals(String dataSource, List<Interval> interval) throws IOException
      {
        return ImmutableList.copyOf(segmentSet);
      }

      @Override
      public List<DataSegment> getUnusedSegmentsForInterval(String dataSource, Interval interval)
      {
        return ImmutableList.of();
      }

      @Override
      public Set<DataSegment> announceHistoricalSegments(Set<DataSegment> segments)
      {
        Set<DataSegment> added = Sets.newHashSet();
        for (final DataSegment segment : segments) {
          if (published.add(segment)) {
            added.add(segment);
          }
        }

        return ImmutableSet.copyOf(added);
      }

      @Override
      public void deleteSegments(Set<DataSegment> segments)
      {
        nuked.addAll(segments);
      }
    };
    final LocalTaskActionClientFactory tac = new LocalTaskActionClientFactory(
        ts,
        new TaskActionToolbox(tl, mdc, newMockEmitter(), EasyMock.createMock(SupervisorManager.class))
    );
    SegmentHandoffNotifierFactory notifierFactory = EasyMock.createNiceMock(SegmentHandoffNotifierFactory.class);
    EasyMock.replay(notifierFactory);

    final TaskToolboxFactory taskToolboxFactory = new TaskToolboxFactory(
        new TaskConfig(tmpDir.getAbsolutePath(), null, null, 50000, null, false, null, null),
        tac,
        newMockEmitter(),
        new DataSegmentPusher()
        {
          @Deprecated
          @Override
          public String getPathForHadoop(String dataSource)
          {
            return getPathForHadoop();
          }

          @Override
          public String getPathForHadoop()
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public DataSegment push(File file, DataSegment segment) throws IOException
          {
            return segment;
          }

          @Override
          public Map<String, Object> makeLoadSpec(URI uri)
          {
            throw new UnsupportedOperationException();
          }
        },
        new DataSegmentKiller()
        {
          @Override
          public void kill(DataSegment segments) throws SegmentLoadingException
          {

          }

          @Override
          public void killAll() throws IOException
          {
            throw new UnsupportedOperationException("not implemented");
          }
        },
        new DataSegmentMover()
        {
          @Override
          public DataSegment move(DataSegment dataSegment, Map<String, Object> targetLoadSpec)
              throws SegmentLoadingException
          {
            return dataSegment;
          }
        },
        new DataSegmentArchiver()
        {
          @Override
          public DataSegment archive(DataSegment segment) throws SegmentLoadingException
          {
            return segment;
          }

          @Override
          public DataSegment restore(DataSegment segment) throws SegmentLoadingException
          {
            return segment;
          }
        },
        null, // segment announcer
        null,
        notifierFactory,
        null, // query runner factory conglomerate corporation unionized collective
        null, // query executor service
        null, // monitor scheduler
        new SegmentLoaderFactory(
            new SegmentLoaderLocalCacheManager(
                null,
                new SegmentLoaderConfig()
                {
                  @Override
                  public List<StorageLocationConfig> getLocations()
                  {
                    return Lists.newArrayList();
                  }
                }, MAPPER
            )
        ),
        MAPPER,
        INDEX_IO,
        null,
        null,
        INDEX_MERGER_V9
    );
    Collection<Object[]> values = new LinkedList<>();
    for (InputRowParser parser : Arrays.<InputRowParser>asList(
        ROW_PARSER,
        new MapInputRowParser(
            new JSONParseSpec(
                new TimestampSpec(TIME_COLUMN, "auto", null),
                new DimensionsSpec(
                    DimensionsSpec.getDefaultSchemas(ImmutableList.<String>of()),
                    ImmutableList.of(DIM_FLOAT_NAME, DIM_LONG_NAME),
                    ImmutableList.<SpatialDimensionSchema>of()
                ),
                null,
                null
            )
        )
    )) {
      for (List<String> dim_names : Arrays.<List<String>>asList(null, ImmutableList.of(DIM_NAME))) {
        for (List<String> metric_names : Arrays.<List<String>>asList(
            null,
            ImmutableList.of(METRIC_LONG_NAME, METRIC_FLOAT_NAME)
        )) {
          values.add(
              new Object[]{
                  new IngestSegmentFirehoseFactory(
                      DATA_SOURCE_NAME,
                      FOREVER,
                      new SelectorDimFilter(DIM_NAME, DIM_VALUE, null),
                      dim_names,
                      metric_names,
                      Guice.createInjector(
                          new Module()
                          {
                            @Override
                            public void configure(Binder binder)
                            {
                              binder.bind(TaskToolboxFactory.class).toInstance(taskToolboxFactory);
                            }
                          }
                      ),
                      INDEX_IO
                  ),
                  StringUtils.format(
                      "DimNames[%s]MetricNames[%s]ParserDimNames[%s]",
                      dim_names == null ? "null" : "dims",
                      metric_names == null ? "null" : "metrics",
                      parser == ROW_PARSER ? "dims" : "null"
                  ),
                  parser
              }
          );
        }
      }
    }
    return values;
  }

  public static ObjectMapper setupInjectablesInObjectMapper(ObjectMapper objectMapper)
  {
    objectMapper.registerModule(
        new SimpleModule("testModule").registerSubtypes(LocalLoadSpec.class)
    );

    final GuiceAnnotationIntrospector guiceIntrospector = new GuiceAnnotationIntrospector();
    objectMapper.setAnnotationIntrospectors(
        new AnnotationIntrospectorPair(
            guiceIntrospector, objectMapper.getSerializationConfig().getAnnotationIntrospector()
        ),
        new AnnotationIntrospectorPair(
            guiceIntrospector, objectMapper.getDeserializationConfig().getAnnotationIntrospector()
        )
    );
    objectMapper.setInjectableValues(
        new GuiceInjectableValues(
            GuiceInjectors.makeStartupInjectorWithModules(
                ImmutableList.of(
                    new Module()
                    {
                      @Override
                      public void configure(Binder binder)
                      {
                        binder.bind(LocalDataSegmentPuller.class);
                      }
                    }
                )
            )
        )
    );
    return objectMapper;
  }

  public IngestSegmentFirehoseFactoryTest(
      IngestSegmentFirehoseFactory factory,
      String testName,
      InputRowParser rowParser
  )
  {
    this.factory = factory;
    this.rowParser = rowParser;
  }

  private static final Logger log = new Logger(IngestSegmentFirehoseFactoryTest.class);
  private static final Interval FOREVER = new Interval(JodaUtils.MIN_INSTANT, JodaUtils.MAX_INSTANT);
  private static final String DATA_SOURCE_NAME = "testDataSource";
  private static final String DATA_SOURCE_VERSION = "version";
  private static final Integer BINARY_VERSION = -1;
  private static final String DIM_NAME = "testDimName";
  private static final String DIM_VALUE = "testDimValue";
  private static final String DIM_LONG_NAME = "testDimLongName";
  private static final String DIM_FLOAT_NAME = "testDimFloatName";
  private static final String METRIC_LONG_NAME = "testLongMetric";
  private static final String METRIC_FLOAT_NAME = "testFloatMetric";
  private static final Long METRIC_LONG_VALUE = 1L;
  private static final Float METRIC_FLOAT_VALUE = 1.0f;
  private static final String TIME_COLUMN = "ts";
  private static final Integer MAX_SHARD_NUMBER = 10;
  private static final Integer MAX_ROWS = 10;
  private static final File tmpDir = Files.createTempDir();
  private static final File persistDir = Paths.get(tmpDir.getAbsolutePath(), "indexTestMerger").toFile();
  private static final List<DataSegment> segmentSet = new ArrayList<>(MAX_SHARD_NUMBER);

  private final IngestSegmentFirehoseFactory factory;
  private final InputRowParser rowParser;

  private static final InputRowParser<Map<String, Object>> ROW_PARSER = new MapInputRowParser(
      new JSONParseSpec(
          new TimestampSpec(TIME_COLUMN, "auto", null),
          new DimensionsSpec(
              DimensionsSpec.getDefaultSchemas(ImmutableList.of(DIM_NAME)),
              ImmutableList.of(DIM_FLOAT_NAME, DIM_LONG_NAME),
              ImmutableList.<SpatialDimensionSchema>of()
          ),
          null,
          null
      )
  );

  private static Map<String, Object> buildRow(Long ts)
  {
    return ImmutableMap.<String, Object>of(
        TIME_COLUMN, ts,
        DIM_NAME, DIM_VALUE,
        DIM_FLOAT_NAME, METRIC_FLOAT_VALUE,
        DIM_LONG_NAME, METRIC_LONG_VALUE
    );
  }

  private static DataSegment buildSegment(Integer shardNumber)
  {
    Preconditions.checkArgument(shardNumber < MAX_SHARD_NUMBER);
    Preconditions.checkArgument(shardNumber >= 0);
    return new DataSegment(
        DATA_SOURCE_NAME,
        FOREVER,
        DATA_SOURCE_VERSION,
        ImmutableMap.<String, Object>of(
            "type", "local",
            "path", persistDir.getAbsolutePath()
        ),
        ImmutableList.of(DIM_NAME),
        ImmutableList.of(METRIC_LONG_NAME, METRIC_FLOAT_NAME),
        new NumberedShardSpec(
            shardNumber,
            MAX_SHARD_NUMBER
        ),
        BINARY_VERSION,
        0L
    );
  }

  @BeforeClass
  public static void setUpStatic() throws IOException
  {
    for (int i = 0; i < MAX_SHARD_NUMBER; ++i) {
      segmentSet.add(buildSegment(i));
    }
  }

  @AfterClass
  public static void tearDownStatic()
  {
    recursivelyDelete(tmpDir);
  }

  private static void recursivelyDelete(final File dir)
  {
    if (dir != null) {
      if (dir.isDirectory()) {
        final File[] files = dir.listFiles();
        if (files != null) {
          for (File file : files) {
            recursivelyDelete(file);
          }
        }
      } else {
        if (!dir.delete()) {
          log.warn("Could not delete file at [%s]", dir.getAbsolutePath());
        }
      }
    }
  }

  @Test
  public void sanityTest()
  {
    Assert.assertEquals(DATA_SOURCE_NAME, factory.getDataSource());
    if (factory.getDimensions() != null) {
      Assert.assertArrayEquals(new String[]{DIM_NAME}, factory.getDimensions().toArray());
    }
    Assert.assertEquals(FOREVER, factory.getInterval());
    if (factory.getMetrics() != null) {
      Assert.assertEquals(
          ImmutableSet.of(METRIC_LONG_NAME, METRIC_FLOAT_NAME),
          ImmutableSet.copyOf(factory.getMetrics())
      );
    }
  }

  @Test
  public void simpleFirehoseReadingTest() throws IOException
  {
    Assert.assertEquals(MAX_SHARD_NUMBER.longValue(), segmentSet.size());
    Integer rowcount = 0;
    try (final IngestSegmentFirehose firehose =
             (IngestSegmentFirehose)
                 factory.connect(rowParser, null)) {
      while (firehose.hasMore()) {
        InputRow row = firehose.nextRow();
        Assert.assertArrayEquals(new String[]{DIM_NAME}, row.getDimensions().toArray());
        Assert.assertArrayEquals(new String[]{DIM_VALUE}, row.getDimension(DIM_NAME).toArray());
        Assert.assertEquals(METRIC_LONG_VALUE.longValue(), row.getLongMetric(METRIC_LONG_NAME));
        Assert.assertEquals(METRIC_FLOAT_VALUE, row.getFloatMetric(METRIC_FLOAT_NAME), METRIC_FLOAT_VALUE * 0.0001);
        ++rowcount;
      }
    }
    Assert.assertEquals((int) MAX_SHARD_NUMBER * MAX_ROWS, (int) rowcount);
  }

  private static ServiceEmitter newMockEmitter()
  {
    return new NoopServiceEmitter();
  }
}
