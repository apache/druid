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
import org.apache.druid.msq.guice.MSQExternalDataSourceModule;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.msq.querykit.DataSegmentProvider;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.ForwardingQueryProcessingPool;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FloatSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.TestGroupByBuffers;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.query.lookup.LookupReferencesManager;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.StorageAdapter;
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
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static org.apache.druid.sql.calcite.util.CalciteTests.DATASOURCE1;
import static org.apache.druid.sql.calcite.util.CalciteTests.DATASOURCE2;
import static org.apache.druid.sql.calcite.util.CalciteTests.DATASOURCE3;
import static org.apache.druid.sql.calcite.util.TestDataBuilder.INDEX_SCHEMA_NUMERIC_DIMS;
import static org.apache.druid.sql.calcite.util.TestDataBuilder.ROWS1;
import static org.apache.druid.sql.calcite.util.TestDataBuilder.ROWS1_WITH_NUMERIC_DIMS;
import static org.apache.druid.sql.calcite.util.TestDataBuilder.ROWS2;

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
          final LookupReferencesManager lookupReferencesManager =
              EasyMock.createStrictMock(LookupReferencesManager.class);
          EasyMock.expect(lookupReferencesManager.getAllLookupNames()).andReturn(Collections.emptySet()).anyTimes();
          EasyMock.replay(lookupReferencesManager);
          binder.bind(LookupReferencesManager.class).toInstance(lookupReferencesManager);
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
          IndexIO indexIO = new IndexIO(testMapper, () -> 0);
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
                .toInstance((dataSegment, channelCounters) -> getSupplierForSegment(dataSegment));

          GroupByQueryConfig groupByQueryConfig = new GroupByQueryConfig();
          binder.bind(GroupByStrategySelector.class)
                .toInstance(GroupByQueryRunnerTest.makeQueryRunnerFactory(groupByQueryConfig, groupByBuffers)
                                                  .getStrategySelector());
        };
    return ImmutableList.of(
        customBindings,
        new IndexingServiceTuningConfigModule(),
        new JoinableFactoryModule(),
        new MSQExternalDataSourceModule(),
        new MSQIndexingModule()
    );
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
              .tmpDir(new File(temporaryFolder.newFolder(), "1"))
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
              .tmpDir(new File(temporaryFolder.newFolder(), "2"))
              .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
              .schema(indexSchemaDifferentDim3M1Types)
              .rows(ROWS2)
              .buildMMappedIndex();
          break;
        case DATASOURCE3:
          index = IndexBuilder
              .create()
              .tmpDir(new File(temporaryFolder.newFolder(), "3"))
              .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
              .schema(INDEX_SCHEMA_NUMERIC_DIMS)
              .rows(ROWS1_WITH_NUMERIC_DIMS)
              .buildMMappedIndex();
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
