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

package org.apache.druid.msq.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.data.input.ResourceInputSource;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.IndexingServiceTuningConfigModule;
import org.apache.druid.guice.JoinableFactoryModule;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.msq.exec.LoadedSegmentDataProvider;
import org.apache.druid.msq.exec.LoadedSegmentDataProviderFactory;
import org.apache.druid.msq.guice.MSQExternalDataSourceModule;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.msq.querykit.DataSegmentProvider;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.ForwardingQueryProcessingPool;
import org.apache.druid.query.NestedDataTestUtils;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FloatSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.GroupingEngine;
import org.apache.druid.query.groupby.TestGroupByBuffers;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPusherConfig;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;
import org.apache.druid.server.coordination.NoopDataSegmentAnnouncer;
import org.apache.druid.sql.calcite.CalciteArraysQueryTest;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.TestDataBuilder;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static org.apache.druid.sql.calcite.util.CalciteTests.DATASOURCE1;
import static org.apache.druid.sql.calcite.util.CalciteTests.DATASOURCE2;
import static org.apache.druid.sql.calcite.util.CalciteTests.DATASOURCE3;
import static org.apache.druid.sql.calcite.util.CalciteTests.DATASOURCE5;
import static org.apache.druid.sql.calcite.util.TestDataBuilder.INDEX_SCHEMA_LOTS_O_COLUMNS;
import static org.apache.druid.sql.calcite.util.TestDataBuilder.INDEX_SCHEMA_NUMERIC_DIMS;
import static org.apache.druid.sql.calcite.util.TestDataBuilder.ROWS1;
import static org.apache.druid.sql.calcite.util.TestDataBuilder.ROWS1_WITH_NUMERIC_DIMS;
import static org.apache.druid.sql.calcite.util.TestDataBuilder.ROWS2;
import static org.apache.druid.sql.calcite.util.TestDataBuilder.ROWS_LOTS_OF_COLUMNS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

/**
 * Helper class aiding in wiring up the Guice bindings required for MSQ engine to work with the Calcite's tests
 */
public class CalciteMSQTestsHelper
{
  public static List<Module> fetchModules(
      TemporaryFolder temporaryFolder,
      TestGroupByBuffers groupByBuffers
  )
  {

    Module customBindings =
        binder -> {
          binder.bind(AppenderatorsManager.class).toProvider(() -> null);

          // Requirements of JoinableFactoryModule
          binder.bind(SegmentManager.class).toInstance(EasyMock.createMock(SegmentManager.class));

          binder.bind(new TypeLiteral<Set<NodeRole>>()
          {
          }).annotatedWith(Self.class).toInstance(ImmutableSet.of(NodeRole.PEON));

          DruidProcessingConfig druidProcessingConfig = new DruidProcessingConfig()
          {
            @Override
            public String getFormatString()
            {
              return "test";
            }
          };
          binder.bind(DruidProcessingConfig.class).toInstance(druidProcessingConfig);
          binder.bind(QueryProcessingPool.class)
                .toInstance(new ForwardingQueryProcessingPool(Execs.singleThreaded("Test-runner-processing-pool")));

          // Select queries donot require this
          Injector dummyInjector = GuiceInjectors.makeStartupInjectorWithModules(
              ImmutableList.of(
                  binder1 -> {
                    binder1.bind(ExprMacroTable.class).toInstance(CalciteTests.createExprMacroTable());
                    binder1.bind(DataSegment.PruneSpecsHolder.class).toInstance(DataSegment.PruneSpecsHolder.DEFAULT);
                  }
              )
          );
          ObjectMapper testMapper = MSQTestBase.setupObjectMapper(dummyInjector);
          IndexIO indexIO = new IndexIO(testMapper, ColumnConfig.DEFAULT);
          SegmentCacheManager segmentCacheManager = null;
          try {
            segmentCacheManager = new SegmentCacheManagerFactory(testMapper).manufacturate(temporaryFolder.newFolder(
                "test"));
          }
          catch (IOException e) {
            e.printStackTrace();
          }
          LocalDataSegmentPusherConfig config = new LocalDataSegmentPusherConfig();
          MSQTestSegmentManager segmentManager = new MSQTestSegmentManager(segmentCacheManager, indexIO);
          try {
            config.storageDirectory = temporaryFolder.newFolder("localsegments");
          }
          catch (IOException e) {
            throw new ISE(e, "Unable to create folder");
          }
          binder.bind(DataSegmentPusher.class).toProvider(() -> new MSQTestDelegateDataSegmentPusher(
              new LocalDataSegmentPusher(config),
              segmentManager
          ));
          binder.bind(DataSegmentAnnouncer.class).toInstance(new NoopDataSegmentAnnouncer());
          binder.bind(DataSegmentProvider.class)
                .toInstance((segmentId, channelCounters, isReindex) -> getSupplierForSegment(segmentId));
          binder.bind(LoadedSegmentDataProviderFactory.class).toInstance(getTestLoadedSegmentDataProviderFactory());

          GroupByQueryConfig groupByQueryConfig = new GroupByQueryConfig();
          GroupingEngine groupingEngine = GroupByQueryRunnerTest.makeQueryRunnerFactory(
              groupByQueryConfig,
              groupByBuffers
          ).getGroupingEngine();
          binder.bind(GroupingEngine.class).toInstance(groupingEngine);
        };
    return ImmutableList.of(
        customBindings,
        new IndexingServiceTuningConfigModule(),
        new JoinableFactoryModule(),
        new MSQExternalDataSourceModule(),
        new MSQIndexingModule()
    );
  }

  private static LoadedSegmentDataProviderFactory getTestLoadedSegmentDataProviderFactory()
  {
    // Currently, there is no metadata in this test for loaded segments. Therefore, this should not be called.
    // In the future, if this needs to be supported, mocks for LoadedSegmentDataProvider should be added like
    // org.apache.druid.msq.exec.MSQLoadedSegmentTests.
    LoadedSegmentDataProviderFactory mockFactory = Mockito.mock(LoadedSegmentDataProviderFactory.class);
    LoadedSegmentDataProvider loadedSegmentDataProvider = Mockito.mock(LoadedSegmentDataProvider.class);
    try {
      doThrow(new AssertionError("Test does not support loaded segment query"))
          .when(loadedSegmentDataProvider).fetchRowsFromDataServer(any(), any(), any(), any());
      doReturn(loadedSegmentDataProvider).when(mockFactory).createLoadedSegmentDataProvider(anyString(), any());
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    return mockFactory;
  }

  private static Supplier<ResourceHolder<Segment>> getSupplierForSegment(SegmentId segmentId)
  {
    final TemporaryFolder temporaryFolder = new TemporaryFolder();
    try {
      temporaryFolder.create();
    }
    catch (IOException e) {
      e.printStackTrace();
    }
    final QueryableIndex index;
    try {
      switch (segmentId.getDataSource()) {
        case DATASOURCE1:
          IncrementalIndexSchema foo1Schema = new IncrementalIndexSchema.Builder()
              .withMetrics(
                  new CountAggregatorFactory("cnt"),
                  new FloatSumAggregatorFactory("m1", "m1"),
                  new DoubleSumAggregatorFactory("m2", "m2"),
                  new HyperUniquesAggregatorFactory("unique_dim1", "dim1")
              )
              .withRollup(false)
              .build();
          index = IndexBuilder
              .create()
              .tmpDir(temporaryFolder.newFolder())
              .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
              .schema(foo1Schema)
              .rows(ROWS1)
              .buildMMappedIndex();
          break;
        case DATASOURCE2:
          final IncrementalIndexSchema indexSchemaDifferentDim3M1Types = new IncrementalIndexSchema.Builder()
              .withDimensionsSpec(
                  new DimensionsSpec(
                      ImmutableList.of(
                          new StringDimensionSchema("dim1"),
                          new StringDimensionSchema("dim2"),
                          new LongDimensionSchema("dim3")
                      )
                  )
              )
              .withMetrics(
                  new CountAggregatorFactory("cnt"),
                  new LongSumAggregatorFactory("m1", "m1"),
                  new DoubleSumAggregatorFactory("m2", "m2"),
                  new HyperUniquesAggregatorFactory("unique_dim1", "dim1")
              )
              .withRollup(false)
              .build();
          index = IndexBuilder
              .create()
              .tmpDir(temporaryFolder.newFolder())
              .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
              .schema(indexSchemaDifferentDim3M1Types)
              .rows(ROWS2)
              .buildMMappedIndex();
          break;
        case DATASOURCE3:
        case CalciteTests.BROADCAST_DATASOURCE:
          index = IndexBuilder
              .create()
              .tmpDir(temporaryFolder.newFolder())
              .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
              .schema(INDEX_SCHEMA_NUMERIC_DIMS)
              .rows(ROWS1_WITH_NUMERIC_DIMS)
              .buildMMappedIndex();
          break;
        case DATASOURCE5:
          index = IndexBuilder
              .create()
              .tmpDir(temporaryFolder.newFolder())
              .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
              .schema(INDEX_SCHEMA_LOTS_O_COLUMNS)
              .rows(ROWS_LOTS_OF_COLUMNS)
              .buildMMappedIndex();
          break;
        case CalciteArraysQueryTest.DATA_SOURCE_ARRAYS:
          index = IndexBuilder.create()
                              .tmpDir(temporaryFolder.newFolder())
                              .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                              .schema(
                                  new IncrementalIndexSchema.Builder()
                                      .withTimestampSpec(NestedDataTestUtils.AUTO_SCHEMA.getTimestampSpec())
                                      .withDimensionsSpec(NestedDataTestUtils.AUTO_SCHEMA.getDimensionsSpec())
                                      .withMetrics(
                                          new CountAggregatorFactory("cnt")
                                      )
                                      .withRollup(false)
                                      .build()
                              )
                              .inputSource(
                                  ResourceInputSource.of(
                                      NestedDataTestUtils.class.getClassLoader(),
                                      NestedDataTestUtils.ARRAY_TYPES_DATA_FILE
                                  )
                              )
                              .inputFormat(TestDataBuilder.DEFAULT_JSON_INPUT_FORMAT)
                              .inputTmpDir(temporaryFolder.newFolder())
                              .buildMMappedIndex();
          break;
        case CalciteTests.WIKIPEDIA_FIRST_LAST:
          index = TestDataBuilder.makeWikipediaIndexWithAggregation(temporaryFolder.newFolder());
          break;
        default:
          throw new ISE("Cannot query segment %s in test runner", segmentId);

      }
    }
    catch (IOException e) {
      throw new ISE(e, "Unable to load index for segment %s", segmentId);
    }
    Segment segment = new Segment()
    {
      @Override
      public SegmentId getId()
      {
        return segmentId;
      }

      @Override
      public Interval getDataInterval()
      {
        return segmentId.getInterval();
      }

      @Nullable
      @Override
      public QueryableIndex asQueryableIndex()
      {
        return index;
      }

      @Override
      public StorageAdapter asStorageAdapter()
      {
        return new QueryableIndexStorageAdapter(index);
      }

      @Override
      public void close()
      {
      }
    };
    return () -> new ReferenceCountingResourceHolder<>(segment, Closer.create());
  }
}
