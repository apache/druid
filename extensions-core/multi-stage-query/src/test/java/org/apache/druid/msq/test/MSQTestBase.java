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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.util.Modules;
import com.google.inject.util.Providers;
import org.apache.druid.client.ImmutableSegmentLoadInfo;
import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.discovery.BrokerClient;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.frame.channel.FrameChannelSequence;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.guice.DruidSecondaryModule;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.IndexingServiceTuningConfigModule;
import org.apache.druid.guice.JoinableFactoryModule;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.NestedDataModule;
import org.apache.druid.guice.SegmentWranglerModule;
import org.apache.druid.guice.StartupInjectorBuilder;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.MSQ;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.common.task.CompactionTask;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.initialization.CoreInjectorBuilder;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.metadata.input.InputSourceModule;
import org.apache.druid.msq.counters.CounterNames;
import org.apache.druid.msq.counters.CounterSnapshots;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.counters.QueryCounterSnapshot;
import org.apache.druid.msq.exec.ClusterStatisticsMergeMode;
import org.apache.druid.msq.exec.Controller;
import org.apache.druid.msq.exec.LoadedSegmentDataProvider;
import org.apache.druid.msq.exec.LoadedSegmentDataProviderFactory;
import org.apache.druid.msq.exec.WorkerMemoryParameters;
import org.apache.druid.msq.guice.MSQDurableStorageModule;
import org.apache.druid.msq.guice.MSQExternalDataSourceModule;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.msq.guice.MSQSqlModule;
import org.apache.druid.msq.guice.MultiStageQuery;
import org.apache.druid.msq.indexing.InputChannelFactory;
import org.apache.druid.msq.indexing.MSQControllerTask;
import org.apache.druid.msq.indexing.MSQSpec;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.msq.indexing.destination.DataSourceMSQDestination;
import org.apache.druid.msq.indexing.destination.TaskReportMSQDestination;
import org.apache.druid.msq.indexing.error.InsertLockPreemptedFaultTest;
import org.apache.druid.msq.indexing.error.MSQErrorReport;
import org.apache.druid.msq.indexing.error.MSQFault;
import org.apache.druid.msq.indexing.error.MSQFaultUtils;
import org.apache.druid.msq.indexing.error.MSQWarnings;
import org.apache.druid.msq.indexing.error.TooManyAttemptsForWorker;
import org.apache.druid.msq.indexing.report.MSQResultsReport;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.querykit.DataSegmentProvider;
import org.apache.druid.msq.shuffle.input.DurableStorageInputChannelFactory;
import org.apache.druid.msq.sql.MSQTaskQueryMaker;
import org.apache.druid.msq.sql.MSQTaskSqlEngine;
import org.apache.druid.msq.sql.entity.PageInformation;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.msq.util.SqlStatementResourceHelper;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.ForwardingQueryProcessingPool;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FloatSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchModule;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.GroupingEngine;
import org.apache.druid.query.groupby.TestGroupByBuffers;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPusherConfig;
import org.apache.druid.segment.loading.LocalLoadSpec;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;
import org.apache.druid.server.coordination.NoopDataSegmentAnnouncer;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.sql.DirectStatement;
import org.apache.druid.sql.SqlQueryPlus;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.SqlToolbox;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.export.TestExportStorageConnector;
import org.apache.druid.sql.calcite.export.TestExportStorageConnectorProvider;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.external.ExternalOperatorConversion;
import org.apache.druid.sql.calcite.external.HttpOperatorConversion;
import org.apache.druid.sql.calcite.external.InlineOperatorConversion;
import org.apache.druid.sql.calcite.external.LocalOperatorConversion;
import org.apache.druid.sql.calcite.planner.CalciteRulesManager;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
import org.apache.druid.sql.calcite.schema.NoopDruidSchemaManager;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.LookylooModule;
import org.apache.druid.sql.calcite.util.QueryFrameworkUtils;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.apache.druid.sql.calcite.view.InProcessViewManager;
import org.apache.druid.sql.guice.SqlBindings;
import org.apache.druid.storage.StorageConfig;
import org.apache.druid.storage.StorageConnector;
import org.apache.druid.storage.StorageConnectorModule;
import org.apache.druid.storage.StorageConnectorProvider;
import org.apache.druid.storage.local.LocalFileStorageConnector;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.PruneLoadSpec;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.druid.timeline.partition.TombstoneShardSpec;
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
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.druid.sql.calcite.util.CalciteTests.DATASOURCE1;
import static org.apache.druid.sql.calcite.util.CalciteTests.DATASOURCE2;
import static org.apache.druid.sql.calcite.util.TestDataBuilder.ROWS1;
import static org.apache.druid.sql.calcite.util.TestDataBuilder.ROWS2;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * Base test runner for running MSQ unit tests. It sets up multi stage query execution environment
 * and populates data for the datasources. The runner does not go via the HTTP layer for communication between the
 * various MSQ processes.
 * <p>
 * Controller -> Coordinator (Coordinator is mocked)
 * <p>
 * In the Ut's we go from:
 * {@link MSQTaskQueryMaker} -> {@link MSQTestOverlordServiceClient} -> {@link Controller}
 * <p>
 * <p>
 * Controller -> Worker communication happens in {@link MSQTestControllerContext}
 * <p>
 * Worker -> Controller communication happens in {@link MSQTestControllerClient}
 * <p>
 * Controller -> Overlord communication happens in {@link MSQTestTaskActionClient}
 */
public class MSQTestBase extends BaseCalciteQueryTest
{
  public static final Map<String, Object> DEFAULT_MSQ_CONTEXT =
      ImmutableMap.<String, Object>builder()
                  .put(QueryContexts.CTX_SQL_QUERY_ID, "test-query")
                  .put(QueryContexts.FINALIZE_KEY, true)
                  .put(QueryContexts.CTX_SQL_STRINGIFY_ARRAYS, false)
                  .put(MultiStageQueryContext.CTX_MAX_NUM_TASKS, 2)
                  .put(MSQWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, 0)
                  .put(MSQTaskQueryMaker.USER_KEY, "allowAll")
                  .build();

  public static final Map<String, Object> DURABLE_STORAGE_MSQ_CONTEXT =
      ImmutableMap.<String, Object>builder()
                  .putAll(DEFAULT_MSQ_CONTEXT)
                  .put(MultiStageQueryContext.CTX_DURABLE_SHUFFLE_STORAGE, true)
                  .build();


  public static final Map<String, Object> FAULT_TOLERANCE_MSQ_CONTEXT =
      ImmutableMap.<String, Object>builder()
                  .putAll(DEFAULT_MSQ_CONTEXT)
                  .put(MultiStageQueryContext.CTX_FAULT_TOLERANCE, true)
                  .build();

  public static final Map<String, Object> PARALLEL_MERGE_MSQ_CONTEXT =
      ImmutableMap.<String, Object>builder()
                  .putAll(DEFAULT_MSQ_CONTEXT)
                  .put(
                      MultiStageQueryContext.CTX_CLUSTER_STATISTICS_MERGE_MODE,
                      ClusterStatisticsMergeMode.PARALLEL.toString()
                  )
                  .build();

  public static final Map<String, Object> FAIL_EMPTY_INSERT_ENABLED_MSQ_CONTEXT =
      ImmutableMap.<String, Object>builder()
                  .putAll(DEFAULT_MSQ_CONTEXT)
                  .put(
                      MultiStageQueryContext.CTX_FAIL_ON_EMPTY_INSERT,
                      true
                  )
                  .build();

  public static final Map<String, Object>
      ROLLUP_CONTEXT_PARAMS = ImmutableMap.<String, Object>builder()
                                          .put(MultiStageQueryContext.CTX_FINALIZE_AGGREGATIONS, false)
                                          .put(GroupByQueryConfig.CTX_KEY_ENABLE_MULTI_VALUE_UNNESTING, false)
                                          .build();

  public static final String FAULT_TOLERANCE = "fault_tolerance";
  public static final String DURABLE_STORAGE = "durable_storage";
  public static final String DEFAULT = "default";
  public static final String PARALLEL_MERGE = "parallel_merge";

  public final boolean useDefault = NullHandling.replaceWithDefault();

  protected File localFileStorageDir;
  protected LocalFileStorageConnector localFileStorageConnector;
  private static final Logger log = new Logger(MSQTestBase.class);
  protected ObjectMapper objectMapper;
  protected MSQTestOverlordServiceClient indexingServiceClient;
  protected MSQTestTaskActionClient testTaskActionClient;
  protected SqlStatementFactory sqlStatementFactory;
  protected AuthorizerMapper authorizerMapper;
  private IndexIO indexIO;
  protected TestExportStorageConnectorProvider exportStorageConnectorProvider = new TestExportStorageConnectorProvider();
  // Contains the metadata of loaded segments
  protected List<ImmutableSegmentLoadInfo> loadedSegmentsMetadata = new ArrayList<>();
  // Mocks the return of data from data servers
  protected LoadedSegmentDataProvider loadedSegmentDataProvider = mock(LoadedSegmentDataProvider.class);

  private MSQTestSegmentManager segmentManager;
  private SegmentCacheManager segmentCacheManager;
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  private TestGroupByBuffers groupByBuffers;
  protected final WorkerMemoryParameters workerMemoryParameters = Mockito.spy(
      WorkerMemoryParameters.createInstance(
          WorkerMemoryParameters.PROCESSING_MINIMUM_BYTES * 50,
          2,
          10,
          2,
          1,
          0
      )
  );

  @Override
  public void configureGuice(DruidInjectorBuilder builder)
  {
    super.configureGuice(builder);

    builder
        .addModule(new HllSketchModule())
        .addModule(new DruidModule()
        {
          // Small subset of MsqSqlModule
          @Override
          public void configure(Binder binder)
          {
            // We want this module to bring InputSourceModule along for the ride.
            binder.install(new InputSourceModule());
            binder.install(new NestedDataModule());
            NestedDataModule.registerHandlersAndSerde();
            SqlBindings.addOperatorConversion(binder, ExternalOperatorConversion.class);
            SqlBindings.addOperatorConversion(binder, HttpOperatorConversion.class);
            SqlBindings.addOperatorConversion(binder, InlineOperatorConversion.class);
            SqlBindings.addOperatorConversion(binder, LocalOperatorConversion.class);
          }

          @Override
          public List<? extends com.fasterxml.jackson.databind.Module> getJacksonModules()
          {
            // We want this module to bring input sources along for the ride.
            return new InputSourceModule().getJacksonModules();
          }
        });
  }

  @After
  public void tearDown2()
  {
    groupByBuffers.close();
  }

  // This test is a Frankenstein creation: it uses the injector set up by the
  // SqlTestFramework to pull items from that are then used to create another
  // injector that has the MSQ dependencies. This allows the test to create a
  // "shadow" statement factory that is used for tests. It works... kinda.
  //
  // Better would be to sort through the Guice stuff and move it into the
  // configureGuice() method above: use the SQL test framework injector so
  // that everything is coordinated. Use the planner factory provided by that
  // framework.
  //
  // Leaving well enough alone for now because any change should be done by
  // someone familiar with the rather complex setup code below.
  //
  // One brute-force attempt ran afoul of circular dependencies: the SQL engine
  // is created in the main injector, but it depends on the SegmentCacheManagerFactory
  // which depends on the object mapper that the injector will provide, once it
  // is built, but has not yet been build while we build the SQL engine.
  @Before
  public void setUp2() throws Exception
  {
    groupByBuffers = TestGroupByBuffers.createDefault();

    SqlTestFramework qf = queryFramework();
    Injector secondInjector = GuiceInjectors.makeStartupInjectorWithModules(
        ImmutableList.of(
            new ExpressionModule(),
            (Module) binder ->
                binder.bind(DataSegment.PruneSpecsHolder.class).toInstance(DataSegment.PruneSpecsHolder.DEFAULT)
        )
    );

    ObjectMapper secondMapper = setupObjectMapper(secondInjector);
    indexIO = new IndexIO(secondMapper, ColumnConfig.DEFAULT);

    try {
      segmentCacheManager = new SegmentCacheManagerFactory(secondMapper).manufacturate(tmpFolder.newFolder("test"));
    }
    catch (IOException exception) {
      throw new ISE(exception, "Unable to create segmentCacheManager");
    }

    MSQSqlModule sqlModule = new MSQSqlModule();

    segmentManager = new MSQTestSegmentManager(segmentCacheManager, indexIO);

    BrokerClient brokerClient = mock(BrokerClient.class);
    List<Module> modules = ImmutableList.of(
        binder -> {
          DruidProcessingConfig druidProcessingConfig = new DruidProcessingConfig()
          {
            @Override
            public String getFormatString()
            {
              return "test";
            }
          };

          GroupByQueryConfig groupByQueryConfig = new GroupByQueryConfig();
          GroupingEngine groupingEngine = GroupByQueryRunnerTest.makeQueryRunnerFactory(
              groupByQueryConfig,
              groupByBuffers
          ).getGroupingEngine();
          binder.bind(GroupingEngine.class).toInstance(groupingEngine);

          binder.bind(DruidProcessingConfig.class).toInstance(druidProcessingConfig);
          binder.bind(new TypeLiteral<Set<NodeRole>>()
          {
          }).annotatedWith(Self.class).toInstance(ImmutableSet.of(NodeRole.PEON));
          binder.bind(QueryProcessingPool.class)
                .toInstance(new ForwardingQueryProcessingPool(Execs.singleThreaded("Test-runner-processing-pool")));
          binder.bind(DataSegmentProvider.class)
                .toInstance((segmentId, channelCounters, isReindex) -> getSupplierForSegment(segmentId));
          binder.bind(LoadedSegmentDataProviderFactory.class).toInstance(getTestLoadedSegmentDataProviderFactory());
          binder.bind(IndexIO.class).toInstance(indexIO);
          binder.bind(SpecificSegmentsQuerySegmentWalker.class).toInstance(qf.walker());

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
            localFileStorageConnector = Mockito.spy(
                new LocalFileStorageConnector(localFileStorageDir)
            );
            binder.bind(Key.get(StorageConnector.class, MultiStageQuery.class))
                  .toProvider(() -> localFileStorageConnector);
            binder.bind(StorageConfig.class).toInstance(new StorageConfig("/"));
          }
          catch (IOException e) {
            throw new ISE(e, "Unable to create setup storage connector");
          }

          binder.bind(DataSegment.PruneSpecsHolder.class).toInstance(DataSegment.PruneSpecsHolder.DEFAULT);
        },
        // Requirement of WorkerMemoryParameters.createProductionInstanceForWorker(injector)
        binder -> binder.bind(AppenderatorsManager.class).toProvider(() -> null),
        // Requirement of JoinableFactoryModule
        binder -> binder.bind(SegmentManager.class).toInstance(EasyMock.createMock(SegmentManager.class)),
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
        new ExpressionModule(),
        new MSQExternalDataSourceModule(),
        new LookylooModule(),
        new SegmentWranglerModule(),
        new HllSketchModule(),
        binder -> binder.bind(BrokerClient.class).toInstance(brokerClient)
    );
    // adding node role injection to the modules, since CliPeon would also do that through run method
    Injector injector = new CoreInjectorBuilder(new StartupInjectorBuilder().build(), ImmutableSet.of(NodeRole.PEON))
        .addAll(modules)
        .build();

    objectMapper = setupObjectMapper(injector);
    objectMapper.registerModule(
        new SimpleModule(StorageConnector.class.getSimpleName())
            .registerSubtypes(
                new NamedType(TestExportStorageConnectorProvider.class, TestExportStorageConnector.TYPE_NAME)
            )
    );
    objectMapper.registerModules(new StorageConnectorModule().getJacksonModules());
    objectMapper.registerModules(sqlModule.getJacksonModules());

    doReturn(mock(Request.class)).when(brokerClient).makeRequest(any(), anyString());

    testTaskActionClient = Mockito.spy(new MSQTestTaskActionClient(objectMapper, injector));
    indexingServiceClient = new MSQTestOverlordServiceClient(
        objectMapper,
        injector,
        testTaskActionClient,
        workerMemoryParameters,
        loadedSegmentsMetadata
    );
    CatalogResolver catalogResolver = createMockCatalogResolver();
    final InProcessViewManager viewManager = new InProcessViewManager(SqlTestFramework.DRUID_VIEW_MACRO_FACTORY);
    DruidSchemaCatalog rootSchema = QueryFrameworkUtils.createMockRootSchema(
        CalciteTests.INJECTOR,
        qf.conglomerate(),
        qf.walker(),
        new PlannerConfig(),
        viewManager,
        new NoopDruidSchemaManager(),
        CalciteTests.TEST_AUTHORIZER_MAPPER,
        CatalogResolver.NULL_RESOLVER
    );

    final SqlEngine engine = new MSQTaskSqlEngine(
        indexingServiceClient,
        qf.queryJsonMapper().copy().registerModules(new MSQSqlModule().getJacksonModules())
    );

    PlannerFactory plannerFactory = new PlannerFactory(
        rootSchema,
        qf.operatorTable(),
        qf.macroTable(),
        PLANNER_CONFIG_DEFAULT,
        CalciteTests.TEST_EXTERNAL_AUTHORIZER_MAPPER,
        objectMapper,
        CalciteTests.DRUID_SCHEMA_NAME,
        new CalciteRulesManager(ImmutableSet.of()),
        CalciteTests.createJoinableFactoryWrapper(),
        catalogResolver,
        new AuthConfig()
    );

    sqlStatementFactory = CalciteTests.createSqlStatementFactory(engine, plannerFactory);

    authorizerMapper = CalciteTests.TEST_EXTERNAL_AUTHORIZER_MAPPER;
  }

  protected CatalogResolver createMockCatalogResolver()
  {
    return CatalogResolver.NULL_RESOLVER;
  }

  /**
   * Returns query context expected for a scan query. Same as {@link #DEFAULT_MSQ_CONTEXT}, but
   * includes {@link DruidQuery#CTX_SCAN_SIGNATURE}.
   */
  protected Map<String, Object> defaultScanQueryContext(Map<String, Object> context, final RowSignature signature)
  {
    try {
      return ImmutableMap.<String, Object>builder()
                         .putAll(context)
                         .put(
                             DruidQuery.CTX_SCAN_SIGNATURE,
                             queryFramework().queryJsonMapper().writeValueAsString(signature)
                         )
                         .build();
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates an array of length and containing values decided by the parameters.
   */
  protected long[] createExpectedFrameArray(int length, int value)
  {
    long[] array = new long[length];
    Arrays.fill(array, value);
    return array;
  }

  private LoadedSegmentDataProviderFactory getTestLoadedSegmentDataProviderFactory()
  {
    LoadedSegmentDataProviderFactory mockFactory = Mockito.mock(LoadedSegmentDataProviderFactory.class);
    doReturn(loadedSegmentDataProvider)
        .when(mockFactory)
        .createLoadedSegmentDataProvider(anyString(), any());
    return mockFactory;
  }

  @Nonnull
  private Supplier<ResourceHolder<Segment>> getSupplierForSegment(SegmentId segmentId)
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
    return () -> ReferenceCountingResourceHolder.fromCloseable(segmentManager.getSegment(segmentId));
  }

  public SelectTester testSelectQuery()
  {
    return new SelectTester();
  }

  public IngestTester testIngestQuery()
  {
    return new IngestTester();
  }

  public static ObjectMapper setupObjectMapper(Injector injector)
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
    DruidSecondaryModule.setupJackson(injector, mapper);

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

  @Nullable
  public static List<Object[]> getRows(@Nullable MSQResultsReport resultsReport)
  {
    if (resultsReport == null) {
      return null;
    } else {
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

      return rows;
    }
  }

  public abstract class MSQTester<Builder extends MSQTester<Builder>>
  {
    protected String sql = null;
    protected Map<String, Object> queryContext = DEFAULT_MSQ_CONTEXT;
    protected List<MSQResultsReport.ColumnAndType> expectedRowSignature = null;
    protected MSQSpec expectedMSQSpec = null;
    protected MSQTuningConfig expectedTuningConfig = null;
    protected Set<SegmentId> expectedSegments = null;
    protected Set<Interval> expectedTombstoneIntervals = null;
    protected List<Object[]> expectedResultRows = null;
    protected Matcher<Throwable> expectedValidationErrorMatcher = null;
    protected List<Pair<Predicate<MSQTaskReportPayload>, String>> adhocReportAssertionAndReasons = new ArrayList<>();
    protected Matcher<Throwable> expectedExecutionErrorMatcher = null;
    protected MSQFault expectedMSQFault = null;
    protected Class<? extends MSQFault> expectedMSQFaultClass = null;
    protected Map<Integer, Integer> expectedStageVsWorkerCount = new HashMap<>();
    protected final Map<Integer, Map<Integer, Map<String, CounterSnapshotMatcher>>>
        expectedStageWorkerChannelToCounters = new HashMap<>();

    private boolean hasRun = false;

    public Builder setSql(String sql)
    {
      this.sql = sql;
      return asBuilder();
    }

    public Builder setQueryContext(Map<String, Object> queryContext)
    {
      this.queryContext = queryContext;
      return asBuilder();
    }

    public Builder setExpectedRowSignature(List<MSQResultsReport.ColumnAndType> expectedRowSignature)
    {
      Preconditions.checkArgument(!expectedRowSignature.isEmpty(), "Row signature cannot be empty");
      this.expectedRowSignature = expectedRowSignature;
      return asBuilder();
    }

    public Builder setExpectedRowSignature(RowSignature expectedRowSignature)
    {
      Preconditions.checkArgument(!expectedRowSignature.equals(RowSignature.empty()), "Row signature cannot be empty");
      this.expectedRowSignature = resultSignatureFromRowSignature(expectedRowSignature);
      return asBuilder();
    }

    public Builder setExpectedSegment(Set<SegmentId> expectedSegments)
    {
      Preconditions.checkArgument(expectedSegments != null, "Segments cannot be null");
      this.expectedSegments = expectedSegments;
      return asBuilder();
    }

    public Builder setExpectedTombstoneIntervals(Set<Interval> tombstoneIntervals)
    {
      Preconditions.checkArgument(!tombstoneIntervals.isEmpty(), "Segments cannot be empty");
      this.expectedTombstoneIntervals = tombstoneIntervals;
      return asBuilder();
    }

    public Builder setExpectedResultRows(List<Object[]> expectedResultRows)
    {
      this.expectedResultRows = expectedResultRows;
      return asBuilder();
    }

    public Builder setExpectedMSQSpec(MSQSpec expectedMSQSpec)
    {
      this.expectedMSQSpec = expectedMSQSpec;
      return asBuilder();
    }

    public Builder addAdhocReportAssertions(Predicate<MSQTaskReportPayload> predicate, String reason)
    {
      this.adhocReportAssertionAndReasons.add(Pair.of(predicate, reason));
      return asBuilder();
    }

    public Builder setExpectedValidationErrorMatcher(Matcher<Throwable> expectedValidationErrorMatcher)
    {
      this.expectedValidationErrorMatcher = expectedValidationErrorMatcher;
      return asBuilder();
    }

    public Builder setExpectedExecutionErrorMatcher(Matcher<Throwable> expectedExecutionErrorMatcher)
    {
      this.expectedExecutionErrorMatcher = expectedExecutionErrorMatcher;
      return asBuilder();
    }

    public Builder setExpectedMSQFault(MSQFault MSQFault)
    {
      this.expectedMSQFault = MSQFault;
      return asBuilder();
    }

    public Builder setExpectedMSQFaultClass(Class<? extends MSQFault> expectedMSQFaultClass)
    {
      this.expectedMSQFaultClass = expectedMSQFaultClass;
      return asBuilder();
    }

    public Builder setExpectedCountersForStageWorkerChannel(
        CounterSnapshotMatcher counterSnapshot,
        int stage,
        int worker,
        String channel
    )
    {
      this.expectedStageWorkerChannelToCounters.computeIfAbsent(stage, s -> new HashMap<>())
                                               .computeIfAbsent(worker, w -> new HashMap<>())
                                               .put(channel, counterSnapshot);
      return asBuilder();
    }

    public Builder setExpectedWorkerCount(Map<Integer, Integer> stageVsWorkerCount)
    {
      this.expectedStageVsWorkerCount = stageVsWorkerCount;
      return asBuilder();
    }

    public Builder setExpectedSegmentGenerationProgressCountersForStageWorker(
        CounterSnapshotMatcher counterSnapshot,
        int stage,
        int worker
    )
    {
      this.expectedStageWorkerChannelToCounters.computeIfAbsent(stage, s -> new HashMap<>())
                                               .computeIfAbsent(worker, w -> new HashMap<>())
                                               .put(CounterNames.getSegmentGenerationProgress(), counterSnapshot);
      return asBuilder();
    }

    @SuppressWarnings("unchecked")
    private Builder asBuilder()
    {
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

    protected void verifyWorkerCount(CounterSnapshotsTree counterSnapshotsTree)
    {
      Map<Integer, Map<Integer, CounterSnapshots>> counterMap = counterSnapshotsTree.copyMap();
      for (Map.Entry<Integer, Integer> stageWorkerCount : expectedStageVsWorkerCount.entrySet()) {
        Assert.assertEquals(stageWorkerCount.getValue().intValue(), counterMap.get(stageWorkerCount.getKey()).size());
      }
    }

    protected void verifyCounters(CounterSnapshotsTree counterSnapshotsTree)
    {
      Assert.assertNotNull(counterSnapshotsTree);

      final Map<Integer, Map<Integer, CounterSnapshots>> stageWorkerToSnapshots = counterSnapshotsTree.copyMap();
      expectedStageWorkerChannelToCounters.forEach((stage, expectedWorkerChannelToCounters) -> {
        final Map<Integer, CounterSnapshots> workerToCounters = stageWorkerToSnapshots.get(stage);
        Assert.assertNotNull("No counters for stage " + stage, workerToCounters);

        expectedWorkerChannelToCounters.forEach((worker, expectedChannelToCounters) -> {
          CounterSnapshots counters = workerToCounters.get(worker);
          Assert.assertNotNull(
              StringUtils.format("No counters for stage [%d], worker [%d]", stage, worker),
              counters
          );

          final Map<String, QueryCounterSnapshot> channelToCounters = counters.getMap();
          expectedChannelToCounters.forEach(
              (channel, counter) -> {
                String errorMessageFormat = StringUtils.format(
                    "Counter mismatch for stage [%d], worker [%d], channel [%s]",
                    stage,
                    worker,
                    channel
                );
                Assert.assertTrue(StringUtils.format(
                    "Counters not found for stage [%d], worker [%d], channel [%s]",
                    stage,
                    worker,
                    channel
                ), channelToCounters.containsKey(channel));
                counter.matchQuerySnapshot(errorMessageFormat, channelToCounters.get(channel));
              }
          );
        });
      });
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
      Preconditions.checkArgument(
          (expectedResultRows != null && expectedResultRows.isEmpty()) || expectedDataSource != null,
          "dataSource cannot be null when expectedResultRows is non-empty"
      );
      Preconditions.checkArgument(
          (expectedResultRows != null && expectedResultRows.isEmpty()) || expectedRowSignature != null,
          "expectedRowSignature cannot be null when expectedResultRows is non-empty"
      );
      Preconditions.checkArgument(
          expectedResultRows != null || expectedMSQFault != null || expectedMSQFaultClass != null,
          "at least one of expectedResultRows, expectedMSQFault or expectedMSQFaultClass should be set to non null"
      );
      Preconditions.checkArgument(expectedShardSpec != null, "shardSpecClass cannot be null");
      readyToRun();
      try {
        String controllerId = runMultiStageQuery(sql, queryContext);
        if (expectedMSQFault != null || expectedMSQFaultClass != null) {
          MSQErrorReport msqErrorReport = getErrorReportOrThrow(controllerId);
          if (expectedMSQFault != null) {
            String errorMessage = msqErrorReport.getFault() instanceof TooManyAttemptsForWorker
                                  ? ((TooManyAttemptsForWorker) msqErrorReport.getFault()).getRootErrorMessage()
                                  : MSQFaultUtils.generateMessageWithErrorCode(msqErrorReport.getFault());
            Assert.assertEquals(
                MSQFaultUtils.generateMessageWithErrorCode(expectedMSQFault),
                errorMessage
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
        MSQTaskReportPayload reportPayload = getPayloadOrThrow(controllerId);
        verifyWorkerCount(reportPayload.getCounters());
        verifyCounters(reportPayload.getCounters());

        MSQSpec foundSpec = indexingServiceClient.getMSQControllerTask(controllerId).getQuerySpec();
        log.info(
            "found generated segments: %s",
            segmentManager.getAllDataSegments().stream().map(s -> s.toString()).collect(
                Collectors.joining("\n"))
        );
        // check if segments are created
        if (!expectedResultRows.isEmpty()) {
          Assert.assertNotEquals(0, segmentManager.getAllDataSegments().size());
        }

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
          Assert.assertEquals(expectedRowSignature, resultSignatureFromRowSignature(storageAdapter.getRowSignature()));

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
            transformedOutputRows.stream().map(Arrays::deepToString).collect(Collectors.joining("\n"))
        );


        // assert data source name when result rows is non-empty
        if (!expectedResultRows.isEmpty()) {
          Assert.assertEquals(expectedDataSource, foundDataSource);
        }
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

        // Assert on the tombstone intervals
        // Tombstone segments are only published, but since they do not have any data, they are not pushed by the
        // SegmentGeneratorFrameProcessorFactory. We can get the tombstone segment ids published by taking a set
        // difference of all the segments published with the segments that are created by the SegmentGeneratorFrameProcessorFactory
        if (!testTaskActionClient.getPublishedSegments().isEmpty()) {
          Set<SegmentId> publishedSegmentIds = testTaskActionClient.getPublishedSegments()
                                                                   .stream()
                                                                   .map(DataSegment::getId)
                                                                   .collect(Collectors.toSet());
          Set<SegmentId> nonEmptySegmentIds = segmentIdVsOutputRowsMap.keySet();
          Set<SegmentId> tombstoneSegmentIds = Sets.difference(publishedSegmentIds, nonEmptySegmentIds);

          // Generate the expected tombstone segment ids
          Map<String, Object> tombstoneLoadSpec = new HashMap<>();
          tombstoneLoadSpec.put("type", DataSegment.TOMBSTONE_LOADSPEC_TYPE);
          tombstoneLoadSpec.put("path", null); // tombstones do not have any backing file
          Set<SegmentId> expectedTombstoneSegmentIds = new HashSet<>();
          if (expectedTombstoneIntervals != null) {
            expectedTombstoneSegmentIds.addAll(
                expectedTombstoneIntervals.stream()
                                          .map(interval -> DataSegment.builder()
                                                                      .dataSource(expectedDataSource)
                                                                      .interval(interval)
                                                                      .version(MSQTestTaskActionClient.VERSION)
                                                                      .shardSpec(new TombstoneShardSpec())
                                                                      .loadSpec(tombstoneLoadSpec)
                                                                      .size(1)
                                                                      .build())
                                          .map(DataSegment::getId)
                                          .collect(Collectors.toSet())
            );
          }
          Assert.assertEquals(expectedTombstoneSegmentIds, tombstoneSegmentIds);
        }

        for (Pair<Predicate<MSQTaskReportPayload>, String> adhocReportAssertionAndReason : adhocReportAssertionAndReasons) {
          Assert.assertTrue(adhocReportAssertionAndReason.rhs, adhocReportAssertionAndReason.lhs.test(reportPayload));
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
    public Pair<MSQSpec, Pair<List<MSQResultsReport.ColumnAndType>, List<Object[]>>> runQueryWithResult()
    {
      readyToRun();
      Preconditions.checkArgument(sql != null, "sql cannot be null");
      Preconditions.checkArgument(queryContext != null, "queryContext cannot be null");

      try {
        String controllerId = runMultiStageQuery(sql, queryContext);

        if (expectedMSQFault != null || expectedMSQFaultClass != null) {
          MSQErrorReport msqErrorReport = getErrorReportOrThrow(controllerId);
          if (expectedMSQFault != null) {
            String errorMessage = msqErrorReport.getFault() instanceof TooManyAttemptsForWorker
                                  ? ((TooManyAttemptsForWorker) msqErrorReport.getFault()).getRootErrorMessage()
                                  : MSQFaultUtils.generateMessageWithErrorCode(msqErrorReport.getFault());
            Assert.assertEquals(
                MSQFaultUtils.generateMessageWithErrorCode(expectedMSQFault),
                errorMessage
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
          MSQControllerTask msqControllerTask = indexingServiceClient.getMSQControllerTask(controllerId);

          final MSQSpec spec = msqControllerTask.getQuerySpec();
          final List<Object[]> rows;

          if (spec.getDestination() instanceof TaskReportMSQDestination) {
            rows = getRows(payload.getResults());
          } else {
            StageDefinition finalStage = Objects.requireNonNull(SqlStatementResourceHelper.getFinalStage(
                payload)).getStageDefinition();

            Optional<List<PageInformation>> pages = SqlStatementResourceHelper.populatePageList(
                payload,
                spec.getDestination()
            );

            if (!pages.isPresent()) {
              throw new ISE("No query results found");
            }

            rows = new ArrayList<>();
            for (PageInformation pageInformation : pages.get()) {
              Closer closer = Closer.create();
              InputChannelFactory inputChannelFactory = DurableStorageInputChannelFactory.createStandardImplementation(
                  controllerId,
                  localFileStorageConnector,
                  closer,
                  true
              );
              rows.addAll(new FrameChannelSequence(inputChannelFactory.openChannel(
                  finalStage.getId(),
                  pageInformation.getWorker() == null ? 0 : pageInformation.getWorker(),
                  pageInformation.getPartition() == null ? 0 : pageInformation.getPartition()
              )).flatMap(frame -> SqlStatementResourceHelper.getResultSequence(
                  msqControllerTask,
                  finalStage,
                  frame,
                  objectMapper
              )).withBaggage(closer).toList());
            }
          }
          if (rows == null) {
            throw new ISE("Query successful but no results found");
          }
          log.info("found row signature %s", payload.getResults().getSignature());
          log.info(rows.stream().map(Arrays::toString).collect(Collectors.joining("\n")));

          for (Pair<Predicate<MSQTaskReportPayload>, String> adhocReportAssertionAndReason : adhocReportAssertionAndReasons) {
            Assert.assertTrue(adhocReportAssertionAndReason.rhs, adhocReportAssertionAndReason.lhs.test(payload));
          }

          log.info("Found spec: %s", objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(spec));

          verifyCounters(payload.getCounters());
          verifyWorkerCount(payload.getCounters());

          return new Pair<>(spec, Pair.of(payload.getResults().getSignature(), rows));
        }
      }
      catch (JsonProcessingException ex) {
        throw new RuntimeException(ex);
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
      if (expectedMSQFault == null) {
        Preconditions.checkArgument(expectedResultRows != null, "Result rows cannot be null");
        Preconditions.checkArgument(expectedRowSignature != null, "Row signature cannot be null");
        Preconditions.checkArgument(expectedMSQSpec != null, "MultiStageQuery Query spec cannot be null ");
      }
      Pair<MSQSpec, Pair<List<MSQResultsReport.ColumnAndType>, List<Object[]>>> specAndResults = runQueryWithResult();

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

  private static List<MSQResultsReport.ColumnAndType> resultSignatureFromRowSignature(final RowSignature signature)
  {
    final List<MSQResultsReport.ColumnAndType> retVal = new ArrayList<>(signature.size());
    for (int i = 0; i < signature.size(); i++) {
      retVal.add(
          new MSQResultsReport.ColumnAndType(
              signature.getColumnName(i),
              signature.getColumnType(i).orElse(null)
          )
      );
    }
    return retVal;
  }
}
