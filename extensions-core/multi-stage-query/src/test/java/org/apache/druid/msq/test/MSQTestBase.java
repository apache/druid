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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteStreams;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.util.Modules;
import com.google.inject.util.Providers;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.IndexingServiceTuningConfigModule;
import org.apache.druid.guice.JoinableFactoryModule;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.MSQ;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.common.task.CompactionTask;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.msq.exec.Controller;
import org.apache.druid.msq.exec.WorkerMemoryParameters;
import org.apache.druid.msq.guice.MSQDurableStorageModule;
import org.apache.druid.msq.guice.MSQExternalDataSourceModule;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.msq.guice.MSQSqlModule;
import org.apache.druid.msq.guice.MultiStageQuery;
import org.apache.druid.msq.indexing.DataSourceMSQDestination;
import org.apache.druid.msq.indexing.MSQSpec;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.msq.indexing.error.InsertLockPreemptedFaultTest;
import org.apache.druid.msq.indexing.error.MSQErrorReport;
import org.apache.druid.msq.indexing.error.MSQFault;
import org.apache.druid.msq.indexing.report.MSQResultsReport;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.msq.querykit.DataSegmentProvider;
import org.apache.druid.msq.querykit.LazyResourceHolder;
import org.apache.druid.msq.sql.MSQTaskQueryMaker;
import org.apache.druid.msq.sql.MSQTaskSqlEngine;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.ForwardingQueryProcessingPool;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FloatSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.expression.LookupEnabledTestExprMacroTable;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.TestGroupByBuffers;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPuller;
import org.apache.druid.segment.loading.LocalDataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPusherConfig;
import org.apache.druid.segment.loading.LocalLoadSpec;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;
import org.apache.druid.server.coordination.NoopDataSegmentAnnouncer;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.sql.DirectStatement;
import org.apache.druid.sql.SqlQueryPlus;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.SqlToolbox;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.planner.CalciteRulesManager;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
import org.apache.druid.sql.calcite.schema.NoopDruidSchemaManager;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.QueryFrameworkUtils;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.apache.druid.sql.calcite.view.InProcessViewManager;
import org.apache.druid.storage.StorageConnector;
import org.apache.druid.storage.StorageConnectorProvider;
import org.apache.druid.storage.local.LocalFileStorageConnectorProvider;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.PruneLoadSpec;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.easymock.EasyMock;
import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.druid.sql.calcite.util.CalciteTests.DATASOURCE1;
import static org.apache.druid.sql.calcite.util.CalciteTests.DATASOURCE2;
import static org.apache.druid.sql.calcite.util.TestDataBuilder.ROWS1;
import static org.apache.druid.sql.calcite.util.TestDataBuilder.ROWS2;

/**
 * Base test runner for running MSQ unit tests. It sets up multi stage query execution environment
 * and populates data for the datasources. The runner does not go via the HTTP layer for communication between the
 * various MSQ processes.
 *
 * Controller -> Coordinator (Coordinator is mocked)
 *
 * In the Ut's we go from:
 * {@link MSQTaskQueryMaker} -> {@link MSQTestOverlordServiceClient} -> {@link Controller}
 *
 *
 * Controller -> Worker communication happens in {@link MSQTestControllerContext}
 *
 * Worker -> Controller communication happens in {@link MSQTestControllerClient}
 *
 * Controller -> Overlord communication happens in {@link MSQTestTaskActionClient}
 */
public class MSQTestBase extends BaseCalciteQueryTest
{
  public static final Map<String, Object> DEFAULT_MSQ_CONTEXT =
      ImmutableMap.<String, Object>builder()
                  .put(MultiStageQueryContext.CTX_ENABLE_DURABLE_SHUFFLE_STORAGE, true)
                  .put(QueryContexts.CTX_SQL_QUERY_ID, "test-query")
                  .put(QueryContexts.FINALIZE_KEY, true)
                  .build();

  public static final Map<String, Object>
      ROLLUP_CONTEXT = ImmutableMap.<String, Object>builder()
                                   .putAll(DEFAULT_MSQ_CONTEXT)
                                   .put(MultiStageQueryContext.CTX_FINALIZE_AGGREGATIONS, false)
                                   .put(GroupByQueryConfig.CTX_KEY_ENABLE_MULTI_VALUE_UNNESTING, false)
                                   .build();

  public final boolean useDefault = NullHandling.replaceWithDefault();

  protected File localFileStorageDir;
  private static final Logger log = new Logger(MSQTestBase.class);
  private ObjectMapper objectMapper;
  private MSQTestOverlordServiceClient indexingServiceClient;
  private SqlStatementFactory sqlStatementFactory;
  private IndexIO indexIO;

  private MSQTestSegmentManager segmentManager;
  private SegmentCacheManager segmentCacheManager;
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  private TestGroupByBuffers groupByBuffers;
  protected final WorkerMemoryParameters workerMemoryParameters = Mockito.spy(WorkerMemoryParameters.compute(
      WorkerMemoryParameters.PROCESSING_MINIMUM_BYTES * 50,
      2,
      10,
      2
  ));

  @After
  public void tearDown2()
  {
    groupByBuffers.close();
  }

  @Before
  public void setUp2()
  {
    groupByBuffers = TestGroupByBuffers.createDefault();

    SqlTestFramework qf = queryFramework();
    Injector secondInjector = GuiceInjectors.makeStartupInjector();


    ObjectMapper secondMapper = setupObjectMapper(secondInjector);
    indexIO = new IndexIO(secondMapper, () -> 0);

    try {
      segmentCacheManager = new SegmentCacheManagerFactory(secondMapper).manufacturate(tmpFolder.newFolder("test"));
    }
    catch (IOException exception) {
      throw new ISE(exception, "Unable to create segmentCacheManager");
    }

    MSQSqlModule sqlModule = new MSQSqlModule();

    segmentManager = new MSQTestSegmentManager(segmentCacheManager, indexIO);

    Injector injector = GuiceInjectors.makeStartupInjectorWithModules(ImmutableList.of(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            DruidProcessingConfig druidProcessingConfig = new DruidProcessingConfig()
            {
              @Override
              public String getFormatString()
              {
                return "test";
              }
            };

            GroupByQueryConfig groupByQueryConfig = new GroupByQueryConfig();

            binder.bind(DruidProcessingConfig.class).toInstance(druidProcessingConfig);
            binder.bind(new TypeLiteral<Set<NodeRole>>()
            {
            }).annotatedWith(Self.class).toInstance(ImmutableSet.of(NodeRole.PEON));
            binder.bind(QueryProcessingPool.class)
                  .toInstance(new ForwardingQueryProcessingPool(Execs.singleThreaded("Test-runner-processing-pool")));
            binder.bind(DataSegmentProvider.class)
                  .toInstance((dataSegment, channelCounters) ->
                                  new LazyResourceHolder<>(getSupplierForSegment(dataSegment)));
            binder.bind(IndexIO.class).toInstance(indexIO);
            binder.bind(SpecificSegmentsQuerySegmentWalker.class).toInstance(qf.walker());

            binder.bind(GroupByStrategySelector.class)
                  .toInstance(GroupByQueryRunnerTest.makeQueryRunnerFactory(groupByQueryConfig, groupByBuffers)
                                                    .getStrategySelector());

            LocalDataSegmentPusherConfig config = new LocalDataSegmentPusherConfig();
            try {
              config.storageDirectory = tmpFolder.newFolder("localsegments");
            }
            catch (IOException e) {
              throw new ISE(e, "Unable to create folder");
            }
            binder.bind(DataSegmentPusher.class).toInstance(new MSQTestDelegateDataSegmentPusher(
                new LocalDataSegmentPusher(config),
                segmentManager
            ));
            binder.bind(DataSegmentAnnouncer.class).toInstance(new NoopDataSegmentAnnouncer());
            binder.bindConstant().annotatedWith(PruneLoadSpec.class).to(false);
            // Client is not used in tests
            binder.bind(Key.get(ServiceClientFactory.class, EscalatedGlobal.class))
                  .toProvider(Providers.of(null));
            // fault tolerance module
            try {
              JsonConfigProvider.bind(
                  binder,
                  MSQDurableStorageModule.MSQ_INTERMEDIATE_STORAGE_PREFIX,
                  StorageConnectorProvider.class,
                  MultiStageQuery.class
              );
              localFileStorageDir = tmpFolder.newFolder("fault");
              binder.bind(Key.get(StorageConnector.class, MultiStageQuery.class))
                    .toProvider(new LocalFileStorageConnectorProvider(localFileStorageDir));
            }
            catch (IOException e) {
              throw new ISE(e, "Unable to create setup storage connector");
            }
          }
        },
        binder -> {
          // Requirements of JoinableFactoryModule
          binder.bind(SegmentManager.class).toInstance(EasyMock.createMock(SegmentManager.class));
          binder.bind(LookupExtractorFactoryContainerProvider.class).toInstance(
              LookupEnabledTestExprMacroTable.createTestLookupProvider(Collections.emptyMap())
          );
        },
        new JoinableFactoryModule(),
        new IndexingServiceTuningConfigModule(),
        new MSQIndexingModule(),
        Modules.override(new MSQSqlModule()).with(
            binder -> {
              // Our Guice configuration currently requires bindings to exist even if they aren't ever used, the
              // following bindings are overriding other bindings that end up needing a lot more dependencies.
              // We replace the bindings with something that returns null to make things more brittle in case they
              // actually are used somewhere in the test.
              binder.bind(SqlStatementFactory.class).annotatedWith(MSQ.class).toProvider(Providers.of(null));
              binder.bind(SqlToolbox.class).toProvider(Providers.of(null));
              binder.bind(MSQTaskSqlEngine.class).toProvider(Providers.of(null));
            }
        ),
        new MSQExternalDataSourceModule()
    ));

    objectMapper = setupObjectMapper(injector);
    objectMapper.registerModules(sqlModule.getJacksonModules());

    indexingServiceClient = new MSQTestOverlordServiceClient(
        objectMapper,
        injector,
        new MSQTestTaskActionClient(objectMapper),
        workerMemoryParameters
    );
    final InProcessViewManager viewManager = new InProcessViewManager(SqlTestFramework.DRUID_VIEW_MACRO_FACTORY);
    DruidSchemaCatalog rootSchema = QueryFrameworkUtils.createMockRootSchema(
        CalciteTests.INJECTOR,
        qf.conglomerate(),
        qf.walker(),
        new PlannerConfig(),
        viewManager,
        new NoopDruidSchemaManager(),
        CalciteTests.TEST_AUTHORIZER_MAPPER
    );

    final SqlEngine engine = new MSQTaskSqlEngine(
        indexingServiceClient,
        qf.queryJsonMapper().copy().registerModules(new MSQSqlModule().getJacksonModules())
    );

    PlannerFactory plannerFactory = new PlannerFactory(
        rootSchema,
        CalciteTests.createOperatorTable(),
        CalciteTests.createExprMacroTable(),
        PLANNER_CONFIG_DEFAULT,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        objectMapper,
        CalciteTests.DRUID_SCHEMA_NAME,
        new CalciteRulesManager(ImmutableSet.of())
    );

    sqlStatementFactory = CalciteTests.createSqlStatementFactory(engine, plannerFactory);
  }

  /**
   * Returns query context expected for a scan query. Same as {@link #DEFAULT_MSQ_CONTEXT}, but
   * includes {@link DruidQuery#CTX_SCAN_SIGNATURE}.
   */
  protected Map<String, Object> defaultScanQueryContext(final RowSignature signature)
  {
    try {
      return ImmutableMap.<String, Object>builder()
                         .putAll(DEFAULT_MSQ_CONTEXT)
                         .put(DruidQuery.CTX_SCAN_SIGNATURE, queryFramework().queryJsonMapper().writeValueAsString(signature))
                         .build();
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Helper method that copies a resource to a temporary file, then returns it.
   */
  protected File getResourceAsTemporaryFile(final String resource) throws IOException
  {
    final File file = temporaryFolder.newFile();
    final InputStream stream = getClass().getResourceAsStream(resource);

    if (stream == null) {
      throw new IOE("No such resource [%s]", resource);
    }

    ByteStreams.copy(stream, Files.newOutputStream(file.toPath()));
    return file;
  }

  @Nonnull
  private Supplier<Pair<Segment, Closeable>> getSupplierForSegment(SegmentId segmentId)
  {
    if (segmentManager.getSegment(segmentId) == null) {
      final QueryableIndex index;
      TemporaryFolder temporaryFolder = new TemporaryFolder();
      try {
        temporaryFolder.create();
      }
      catch (IOException e) {
        throw new ISE(e, "Unable to create temporary folder for tests");
      }
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
                .tmpDir(new File(temporaryFolder.newFolder(), "1"))
                .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                .schema(indexSchemaDifferentDim3M1Types)
                .rows(ROWS2)
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
      segmentManager.addSegment(segment);
    }
    return new Supplier<Pair<Segment, Closeable>>()
    {
      @Override
      public Pair<Segment, Closeable> get()
      {
        return new Pair<>(segmentManager.getSegment(segmentId), Closer.create());
      }
    };
  }

  public SelectTester testSelectQuery()
  {
    return new SelectTester();
  }

  public IngestTester testIngestQuery()
  {
    return new IngestTester();
  }

  private ObjectMapper setupObjectMapper(Injector injector)
  {
    ObjectMapper mapper = injector.getInstance(ObjectMapper.class)
                                  .registerModules(new SimpleModule(IndexingServiceTuningConfigModule.class.getSimpleName())
                                                       .registerSubtypes(
                                                           new NamedType(IndexTask.IndexTuningConfig.class, "index"),
                                                           new NamedType(
                                                               ParallelIndexTuningConfig.class,
                                                               "index_parallel"
                                                           ),
                                                           new NamedType(
                                                               CompactionTask.CompactionTuningConfig.class,
                                                               "compaction"
                                                           )
                                                       ).registerSubtypes(ExternalDataSource.class));
    mapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(ObjectMapper.class, mapper)
            .addValue(Injector.class, injector)
            .addValue(DataSegment.PruneSpecsHolder.class, DataSegment.PruneSpecsHolder.DEFAULT)
            .addValue(LocalDataSegmentPuller.class, new LocalDataSegmentPuller())
            .addValue(ExprMacroTable.class, CalciteTests.createExprMacroTable())
    );

    mapper.registerSubtypes(new NamedType(LocalLoadSpec.class, "local"));

    // This should be reusing guice instead of using static classes
    InsertLockPreemptedFaultTest.LockPreemptedHelper.preempt(false);

    return mapper;
  }

  private String runMultiStageQuery(String query, Map<String, Object> context)
  {
    final DirectStatement stmt = sqlStatementFactory.directStatement(
        new SqlQueryPlus(
            query,
            context,
            Collections.emptyList(),
            CalciteTests.REGULAR_USER_AUTH_RESULT
        )
    );

    final List<Object[]> sequence = stmt.execute().getResults().toList();
    return (String) Iterables.getOnlyElement(sequence)[0];
  }

  private MSQTaskReportPayload getPayloadOrThrow(String controllerTaskId)
  {
    MSQTaskReportPayload payload =
        (MSQTaskReportPayload) indexingServiceClient.getReportForTask(controllerTaskId)
                                                    .get(MSQTaskReport.REPORT_KEY)
                                                    .getPayload();
    if (payload.getStatus().getStatus().isFailure()) {
      throw new ISE(
          "Query task [%s] failed due to %s",
          controllerTaskId,
          payload.getStatus().getErrorReport().toString()
      );
    }

    if (!payload.getStatus().getStatus().isComplete()) {
      throw new ISE("Query task [%s] should have finished", controllerTaskId);
    }

    return payload;
  }

  private MSQErrorReport getErrorReportOrThrow(String controllerTaskId)
  {
    MSQTaskReportPayload payload =
        (MSQTaskReportPayload) indexingServiceClient.getReportForTask(controllerTaskId)
                                                    .get(MSQTaskReport.REPORT_KEY)
                                                    .getPayload();
    if (!payload.getStatus().getStatus().isFailure()) {
      throw new ISE(
          "Query task [%s] was supposed to fail",
          controllerTaskId
      );
    }

    if (!payload.getStatus().getStatus().isComplete()) {
      throw new ISE("Query task [%s] should have finished", controllerTaskId);
    }

    return payload.getStatus().getErrorReport();

  }

  private void assertMSQSpec(MSQSpec expectedMSQSpec, MSQSpec querySpecForTask)
  {
    Assert.assertEquals(expectedMSQSpec.getQuery(), querySpecForTask.getQuery());
    Assert.assertEquals(expectedMSQSpec.getAssignmentStrategy(), querySpecForTask.getAssignmentStrategy());
    Assert.assertEquals(expectedMSQSpec.getColumnMappings(), querySpecForTask.getColumnMappings());
    Assert.assertEquals(expectedMSQSpec.getDestination(), querySpecForTask.getDestination());
  }

  private void assertTuningConfig(
      MSQTuningConfig expectedTuningConfig,
      MSQTuningConfig tuningConfig
  )
  {
    Assert.assertEquals(
        expectedTuningConfig.getMaxNumWorkers(),
        tuningConfig.getMaxRowsInMemory()
    );
    Assert.assertEquals(
        expectedTuningConfig.getMaxRowsInMemory(),
        tuningConfig.getMaxRowsInMemory()
    );
    Assert.assertEquals(
        expectedTuningConfig.getRowsPerSegment(),
        tuningConfig.getRowsPerSegment()
    );
  }

  private Optional<Pair<RowSignature, List<Object[]>>> getSignatureWithRows(MSQResultsReport resultsReport)
  {
    if (resultsReport == null) {
      return Optional.empty();
    } else {
      RowSignature rowSignature = resultsReport.getSignature();
      Yielder<Object[]> yielder = resultsReport.getResultYielder();
      List<Object[]> rows = new ArrayList<>();
      while (!yielder.isDone()) {
        rows.add(yielder.get());
        yielder = yielder.next(null);
      }
      try {
        yielder.close();
      }
      catch (IOException e) {
        throw new ISE("Unable to get results from the report");
      }

      return Optional.of(new Pair(rowSignature, rows));
    }
  }

  public abstract class MSQTester<Builder extends MSQTester<?>>
  {
    protected String sql = null;
    protected Map<String, Object> queryContext = DEFAULT_MSQ_CONTEXT;
    protected RowSignature expectedRowSignature = null;
    protected MSQSpec expectedMSQSpec = null;
    protected MSQTuningConfig expectedTuningConfig = null;
    protected Set<SegmentId> expectedSegments = null;
    protected List<Object[]> expectedResultRows = null;
    protected Matcher<Throwable> expectedValidationErrorMatcher = null;
    protected Matcher<Throwable> expectedExecutionErrorMatcher = null;
    protected MSQFault expectedMSQFault = null;
    protected Class<? extends MSQFault> expectedMSQFaultClass = null;

    private boolean hasRun = false;

    @SuppressWarnings("unchecked")
    public Builder setSql(String sql)
    {
      this.sql = sql;
      return (Builder) this;
    }

    @SuppressWarnings("unchecked")
    public Builder setQueryContext(Map<String, Object> queryContext)
    {
      this.queryContext = queryContext;
      return (Builder) this;
    }

    @SuppressWarnings("unchecked")
    public Builder setExpectedRowSignature(RowSignature expectedRowSignature)
    {
      Preconditions.checkArgument(!expectedRowSignature.equals(RowSignature.empty()), "Row signature cannot be empty");
      this.expectedRowSignature = expectedRowSignature;
      return (Builder) this;
    }

    @SuppressWarnings("unchecked")
    public Builder setExpectedSegment(Set<SegmentId> expectedSegments)
    {
      Preconditions.checkArgument(!expectedSegments.isEmpty(), "Segments cannot be empty");
      this.expectedSegments = expectedSegments;
      return (Builder) this;
    }

    @SuppressWarnings("unchecked")
    public Builder setExpectedResultRows(List<Object[]> expectedResultRows)
    {
      Preconditions.checkArgument(expectedResultRows.size() > 0, "Results rows cannot be empty");
      this.expectedResultRows = expectedResultRows;
      return (Builder) this;
    }

    @SuppressWarnings("unchecked")
    public Builder setExpectedMSQSpec(MSQSpec expectedMSQSpec)
    {
      this.expectedMSQSpec = expectedMSQSpec;
      return (Builder) this;
    }

    @SuppressWarnings("unchecked")
    public Builder setExpectedValidationErrorMatcher(Matcher<Throwable> expectedValidationErrorMatcher)
    {
      this.expectedValidationErrorMatcher = expectedValidationErrorMatcher;
      return (Builder) this;
    }

    @SuppressWarnings("unchecked")
    public Builder setExpectedExecutionErrorMatcher(Matcher<Throwable> expectedExecutionErrorMatcher)
    {
      this.expectedExecutionErrorMatcher = expectedExecutionErrorMatcher;
      return (Builder) this;
    }

    @SuppressWarnings("unchecked")
    public Builder setExpectedMSQFault(MSQFault MSQFault)
    {
      this.expectedMSQFault = MSQFault;
      return (Builder) this;
    }

    public Builder setExpectedMSQFaultClass(Class<? extends MSQFault> expectedMSQFaultClass)
    {
      this.expectedMSQFaultClass = expectedMSQFaultClass;
      return (Builder) this;
    }

    public void verifyPlanningErrors()
    {
      Preconditions.checkArgument(expectedValidationErrorMatcher != null, "Validation error matcher cannot be null");
      Preconditions.checkArgument(sql != null, "Sql cannot be null");
      readyToRun();

      final Throwable e = Assert.assertThrows(
          Throwable.class,
          () -> runMultiStageQuery(sql, queryContext)
      );

      MatcherAssert.assertThat(e, expectedValidationErrorMatcher);
    }

    protected void readyToRun()
    {
      if (!hasRun) {
        hasRun = true;
      } else {
        throw new ISE("Use one @Test method per tester");
      }
    }
  }

  public class IngestTester extends MSQTester<IngestTester>
  {
    private String expectedDataSource;

    private Class<? extends ShardSpec> expectedShardSpec = NumberedShardSpec.class;

    private boolean expectedRollUp = false;

    private Granularity expectedQueryGranularity = Granularities.NONE;

    private List<AggregatorFactory> expectedAggregatorFactories = new ArrayList<>();

    private List<Interval> expectedDestinationIntervals = null;

    private IngestTester()
    {
      // nothing to do
    }

    public IngestTester setExpectedDataSource(String expectedDataSource)
    {
      this.expectedDataSource = expectedDataSource;
      return this;
    }

    public IngestTester setExpectedShardSpec(Class<? extends ShardSpec> expectedShardSpec)
    {
      this.expectedShardSpec = expectedShardSpec;
      return this;
    }

    public IngestTester setExpectedDestinationIntervals(List<Interval> expectedDestinationIntervals)
    {
      this.expectedDestinationIntervals = expectedDestinationIntervals;
      return this;
    }

    public IngestTester setExpectedRollUp(boolean expectedRollUp)
    {
      this.expectedRollUp = expectedRollUp;
      return this;
    }

    public IngestTester setExpectedQueryGranularity(Granularity expectedQueryGranularity)
    {
      this.expectedQueryGranularity = expectedQueryGranularity;
      return this;
    }

    public IngestTester addExpectedAggregatorFactory(AggregatorFactory aggregatorFactory)
    {
      expectedAggregatorFactories.add(aggregatorFactory);
      return this;
    }

    public void verifyResults()
    {
      Preconditions.checkArgument(sql != null, "sql cannot be null");
      Preconditions.checkArgument(queryContext != null, "queryContext cannot be null");
      Preconditions.checkArgument(expectedDataSource != null, "dataSource cannot be null");
      Preconditions.checkArgument(expectedRowSignature != null, "expectedRowSignature cannot be null");
      Preconditions.checkArgument(
          expectedResultRows != null || expectedMSQFault != null || expectedMSQFaultClass != null,
          "atleast one of expectedResultRows, expectedMSQFault or expectedMSQFaultClass should be set to non null"
      );
      Preconditions.checkArgument(expectedShardSpec != null, "shardSpecClass cannot be null");
      readyToRun();
      try {
        String controllerId = runMultiStageQuery(sql, queryContext);
        if (expectedMSQFault != null || expectedMSQFaultClass != null) {
          MSQErrorReport msqErrorReport = getErrorReportOrThrow(controllerId);
          if (expectedMSQFault != null) {
            Assert.assertEquals(
                expectedMSQFault.getCodeWithMessage(),
                msqErrorReport.getFault().getCodeWithMessage()
            );
          }
          if (expectedMSQFaultClass != null) {
            Assert.assertEquals(
                expectedMSQFaultClass,
                msqErrorReport.getFault().getClass()
            );
          }

          return;
        }
        getPayloadOrThrow(controllerId);
        MSQSpec foundSpec = indexingServiceClient.getQuerySpecForTask(controllerId);
        log.info(
            "found generated segments: %s",
            segmentManager.getAllDataSegments().stream().map(s -> s.toString()).collect(
                Collectors.joining("\n"))
        );
        //check if segments are created
        Assert.assertNotEquals(0, segmentManager.getAllDataSegments().size());


        String foundDataSource = null;
        SortedMap<SegmentId, List<List<Object>>> segmentIdVsOutputRowsMap = new TreeMap<>();
        for (DataSegment dataSegment : segmentManager.getAllDataSegments()) {

          //Assert shard spec class
          Assert.assertEquals(expectedShardSpec, dataSegment.getShardSpec().getClass());
          if (foundDataSource == null) {
            foundDataSource = dataSegment.getDataSource();

          } else if (!foundDataSource.equals(dataSegment.getDataSource())) {
            throw new ISE(
                "Expected only one datasource in the list of generated segments found [%s,%s]",
                foundDataSource,
                dataSegment.getDataSource()
            );
          }
          final QueryableIndex queryableIndex = indexIO.loadIndex(segmentCacheManager.getSegmentFiles(
              dataSegment));
          final StorageAdapter storageAdapter = new QueryableIndexStorageAdapter(queryableIndex);

          // assert rowSignature
          Assert.assertEquals(expectedRowSignature, storageAdapter.getRowSignature());

          // assert rollup
          Assert.assertEquals(expectedRollUp, queryableIndex.getMetadata().isRollup());

          // asset query granularity
          Assert.assertEquals(expectedQueryGranularity, queryableIndex.getMetadata().getQueryGranularity());

          // assert aggregator factories
          Assert.assertArrayEquals(
              expectedAggregatorFactories.toArray(new AggregatorFactory[0]),
              queryableIndex.getMetadata().getAggregators()
          );

          for (List<Object> row : FrameTestUtil.readRowsFromAdapter(storageAdapter, null, false).toList()) {
            // transforming rows for sketch assertions
            List<Object> transformedRow = row.stream()
                                             .map(r -> {
                                               if (r instanceof HyperLogLogCollector) {
                                                 return ((HyperLogLogCollector) r).estimateCardinalityRound();
                                               } else {
                                                 return r;
                                               }
                                             })
                                             .collect(Collectors.toList());
            segmentIdVsOutputRowsMap.computeIfAbsent(dataSegment.getId(), r -> new ArrayList<>()).add(transformedRow);
          }
        }

        log.info("Found spec: %s", objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(foundSpec));
        List<Object[]> transformedOutputRows = segmentIdVsOutputRowsMap.values()
                                                                       .stream()
                                                                       .flatMap(Collection::stream)
                                                                       .map(List::toArray)
                                                                       .collect(Collectors.toList());

        log.info(
            "Found rows which are sorted forcefully %s",
            transformedOutputRows.stream().map(a -> Arrays.toString(a)).collect(Collectors.joining("\n"))
        );


        // assert data source name
        Assert.assertEquals(expectedDataSource, foundDataSource);
        // assert spec
        if (expectedMSQSpec != null) {
          assertMSQSpec(expectedMSQSpec, foundSpec);
        }
        if (expectedTuningConfig != null) {
          assertTuningConfig(expectedTuningConfig, foundSpec.getTuningConfig());
        }
        if (expectedDestinationIntervals != null) {
          Assert.assertNotNull(foundSpec);
          DataSourceMSQDestination destination = (DataSourceMSQDestination) foundSpec.getDestination();
          Assert.assertEquals(expectedDestinationIntervals, destination.getReplaceTimeChunks());
        }
        if (expectedSegments != null) {
          Assert.assertEquals(expectedSegments, segmentIdVsOutputRowsMap.keySet());
          for (Object[] row : transformedOutputRows) {
            List<SegmentId> diskSegmentList = segmentIdVsOutputRowsMap.keySet()
                                                                      .stream()
                                                                      .filter(segmentId -> segmentId.getInterval()
                                                                                                    .contains((Long) row[0]))
                                                                      .collect(Collectors.toList());
            if (diskSegmentList.size() != 1) {
              throw new IllegalStateException("Single key in multiple partitions");
            }
            SegmentId diskSegment = diskSegmentList.get(0);
            // Checking if the row belongs to the correct segment interval
            Assert.assertTrue(segmentIdVsOutputRowsMap.get(diskSegment).contains(Arrays.asList(row)));
          }
        }
        // assert results
        assertResultsEquals(sql, expectedResultRows, transformedOutputRows);
      }
      catch (Exception e) {
        throw new ISE(e, "Query %s failed", sql);
      }
    }

    public void verifyExecutionError()
    {
      Preconditions.checkArgument(sql != null, "sql cannot be null");
      Preconditions.checkArgument(queryContext != null, "queryContext cannot be null");
      Preconditions.checkArgument(expectedExecutionErrorMatcher != null, "Execution error matcher cannot be null");
      readyToRun();
      try {
        String controllerId = runMultiStageQuery(sql, queryContext);
        getPayloadOrThrow(controllerId);
        Assert.fail(StringUtils.format("Query did not throw an exception (sql = [%s])", sql));
      }
      catch (Exception e) {
        MatcherAssert.assertThat(
            StringUtils.format("Query error did not match expectations (sql = [%s])", sql),
            e,
            expectedExecutionErrorMatcher
        );
      }
    }
  }

  public class SelectTester extends MSQTester<SelectTester>
  {
    private SelectTester()
    {
      // nothing to do
    }

    // Made the visibility public to aid adding ut's easily with minimum parameters to set.
    @Nullable
    public Pair<MSQSpec, Pair<RowSignature, List<Object[]>>> runQueryWithResult()
    {
      readyToRun();
      Preconditions.checkArgument(sql != null, "sql cannot be null");
      Preconditions.checkArgument(queryContext != null, "queryContext cannot be null");

      try {
        String controllerId = runMultiStageQuery(sql, queryContext);

        if (expectedMSQFault != null || expectedMSQFaultClass != null) {
          MSQErrorReport msqErrorReport = getErrorReportOrThrow(controllerId);
          if (expectedMSQFault != null) {
            Assert.assertEquals(
                expectedMSQFault.getCodeWithMessage(),
                msqErrorReport.getFault().getCodeWithMessage()
            );
          }
          if (expectedMSQFaultClass != null) {
            Assert.assertEquals(
                expectedMSQFaultClass,
                msqErrorReport.getFault().getClass()
            );
          }
          return null;
        }

        MSQTaskReportPayload payload = getPayloadOrThrow(controllerId);

        if (payload.getStatus().getErrorReport() != null) {
          throw new ISE("Query %s failed due to %s", sql, payload.getStatus().getErrorReport().toString());
        } else {
          Optional<Pair<RowSignature, List<Object[]>>> rowSignatureListPair = getSignatureWithRows(payload.getResults());
          if (!rowSignatureListPair.isPresent()) {
            throw new ISE("Query successful but no results found");
          }
          log.info("found row signature %s", rowSignatureListPair.get().lhs);
          log.info(rowSignatureListPair.get().rhs.stream()
                                                 .map(row -> Arrays.toString(row))
                                                 .collect(Collectors.joining("\n")));

          MSQSpec spec = indexingServiceClient.getQuerySpecForTask(controllerId);
          log.info("Found spec: %s", objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(spec));
          return new Pair<>(spec, rowSignatureListPair.get());
        }
      }
      catch (Exception e) {
        if (expectedExecutionErrorMatcher == null) {
          throw new ISE(e, "Query %s failed", sql);
        }
        MatcherAssert.assertThat(e, expectedExecutionErrorMatcher);
        return null;
      }
    }

    public void verifyResults()
    {
      Preconditions.checkArgument(expectedResultRows != null, "Result rows cannot be null");
      Preconditions.checkArgument(expectedRowSignature != null, "Row signature cannot be null");
      Preconditions.checkArgument(expectedMSQSpec != null, "MultiStageQuery Query spec not ");
      Pair<MSQSpec, Pair<RowSignature, List<Object[]>>> specAndResults = runQueryWithResult();

      if (specAndResults == null) { // A fault was expected and the assertion has been done in the runQueryWithResult
        return;
      }

      Assert.assertEquals(expectedRowSignature, specAndResults.rhs.lhs);
      assertResultsEquals(sql, expectedResultRows, specAndResults.rhs.rhs);
      assertMSQSpec(expectedMSQSpec, specAndResults.lhs);
    }

    public void verifyExecutionError()
    {
      Preconditions.checkArgument(expectedExecutionErrorMatcher != null, "Execution error matcher cannot be null");
      if (runQueryWithResult() != null) {
        throw new ISE("Query %s did not throw an exception", sql);
      }
    }
  }
}
