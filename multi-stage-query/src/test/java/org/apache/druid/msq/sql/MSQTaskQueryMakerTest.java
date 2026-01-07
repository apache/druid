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

package org.apache.druid.msq.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.testing.fieldbinder.Bind;
import com.google.inject.testing.fieldbinder.BoundFieldModule;
import com.google.inject.util.Modules;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.guice.ConfigModule;
import org.apache.druid.guice.DruidGuiceExtensions;
import org.apache.druid.guice.DruidSecondaryModule;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.SegmentWranglerModule;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.msq.exec.DataServerQueryHandlerFactory;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.msq.indexing.destination.MSQTerminalStageSpecFactory;
import org.apache.druid.msq.indexing.destination.SegmentGenerationTerminalStageSpecFactory;
import org.apache.druid.msq.indexing.error.MSQErrorReport;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.msq.querykit.DataSegmentProvider;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.msq.test.MSQTestOverlordServiceClient;
import org.apache.druid.msq.test.MSQTestTaskActionClient;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.Druids;
import org.apache.druid.query.ForwardingQueryProcessingPool;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.RestrictedDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnnestDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.GroupingEngine;
import org.apache.druid.query.groupby.TestGroupByBuffers;
import org.apache.druid.query.policy.NoopPolicyEnforcer;
import org.apache.druid.query.policy.PolicyEnforcer;
import org.apache.druid.query.policy.RestrictAllTablesPolicyEnforcer;
import org.apache.druid.query.policy.RowFilterPolicy;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.server.QueryResponse;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.server.lookup.cache.LookupLoadingSpec;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.LookylooModule;
import org.apache.druid.sql.calcite.util.TestDataBuilder;
import org.apache.druid.sql.destination.IngestDestination;
import org.apache.druid.timeline.SegmentId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toMap;
import static org.apache.druid.sql.calcite.BaseCalciteQueryTest.assertResultsEquals;
import static org.apache.druid.sql.calcite.BaseCalciteQueryTest.expressionVirtualColumn;
import static org.apache.druid.sql.calcite.table.RowSignatures.toRelDataType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.when;

public class MSQTaskQueryMakerTest
{
  private static final Closer CLOSER = Closer.create();
  private static final JavaTypeFactoryImpl JAVA_TYPE_FACTORY = new JavaTypeFactoryImpl();

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Bind
  private SpecificSegmentsQuerySegmentWalker walker;
  @Bind
  @Mock
  private DataSegmentProvider dataSegmentProviderMock;
  @Bind
  private ObjectMapper objectMapper;
  @Bind
  @Json
  private ObjectMapper jsonMapper;
  @Bind
  private IndexIO indexIO;
  @Bind
  @Nullable
  private IngestDestination ingestDestination;
  @Bind
  private QueryProcessingPool queryProcessingPool;
  @Bind
  private GroupingEngine groupingEngine;
  @Bind
  private JoinableFactoryWrapper joinableFactoryWrapper;
  @Bind
  @Nullable
  private DataServerQueryHandlerFactory dataServerQueryHandlerFactory;
  @Bind(lazy = true)
  private PolicyEnforcer policyEnforcer; // lazy so we can set it in the test
  @Bind
  private MSQTerminalStageSpecFactory terminalStageSpecFactory;
  @Bind
  @Mock
  private PlannerContext plannerContextMock;
  @Bind(lazy = true)
  private List<Map.Entry<Integer, String>> fieldMapping; // lazy so we can set it in the test
  @Bind(lazy = true)
  private OverlordClient fakeOverlordClient; // lazy since we need to use the injector to create it

  private MSQTaskQueryMaker msqTaskQueryMaker;

  @Before
  public void setUp() throws Exception
  {
    walker = TestDataBuilder.addDataSetsToWalker(
        FileUtils.getTempDir().toFile(),
        SpecificSegmentsQuerySegmentWalker.createWalker(QueryStackTests.createQueryRunnerFactoryConglomerate(CLOSER))
    );
    when(dataSegmentProviderMock.fetchSegment(
        any(),
        any(),
        anyBoolean()
    )).thenAnswer(invocation -> (Supplier<?>) () -> {
      SegmentId segmentId = (SegmentId) invocation.getArguments()[0];
      return new ReferenceCountingResourceHolder(walker.getSegment(segmentId), () -> {
        // no-op closer, we don't want to close the segment
      });
    });

    objectMapper = TestHelper.makeJsonMapper();
    jsonMapper = new DefaultObjectMapper();
    indexIO = new IndexIO(objectMapper, ColumnConfig.DEFAULT);
    queryProcessingPool = new ForwardingQueryProcessingPool(Execs.singleThreaded("Test-runner-processing-pool"));
    groupingEngine = GroupByQueryRunnerTest.makeQueryRunnerFactory(
        new GroupByQueryConfig(),
        TestGroupByBuffers.createDefault()
    ).getGroupingEngine();
    joinableFactoryWrapper = CalciteTests.createJoinableFactoryWrapper();
    policyEnforcer = NoopPolicyEnforcer.instance();
    terminalStageSpecFactory = new SegmentGenerationTerminalStageSpecFactory();
    when(plannerContextMock.getLookupLoadingSpec()).thenReturn(LookupLoadingSpec.NONE);
    when(plannerContextMock.queryContext()).thenReturn(new QueryContext(ImmutableMap.of()));
    when(plannerContextMock.getSql()).thenReturn("stub a sql statement, ignore this value");
    when(plannerContextMock.getJsonMapper()).thenReturn(jsonMapper);
    when(plannerContextMock.getAuthenticationResult()).thenReturn(new AuthenticationResult(
        "someone",
        "ignore",
        "ignore",
        ImmutableMap.of()
    ));

    Module defaultModule = Modules.combine(
        new ExpressionModule(),
        new DruidGuiceExtensions(),
        new LifecycleModule(),
        new ConfigModule(),
        new SegmentWranglerModule(),
        new LookylooModule(),
        new MSQIndexingModule()
    );
    Injector injector = Guice.createInjector(defaultModule, BoundFieldModule.of(this));
    DruidSecondaryModule.setupJackson(injector, objectMapper);
    fakeOverlordClient = new MSQTestOverlordServiceClient(
        objectMapper,
        injector,
        new MSQTestTaskActionClient(objectMapper, injector),
        MSQTestBase.makeTestWorkerMemoryParameters(),
        new ArrayList<>()
    );
  }

  @Test
  public void testSimpleScanQuery() throws Exception
  {
    // Arrange
    RowSignature resultSignature = RowSignature.builder()
                                               .add("cnt", ColumnType.LONG)
                                               .add("dim1", ColumnType.STRING)
                                               .build();
    fieldMapping = buildFieldMapping(resultSignature);
    Query query = new Druids.ScanQueryBuilder().resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                               .eternityInterval()
                                               .dataSource(CalciteTests.DATASOURCE1)
                                               .columns(resultSignature.getColumnNames())
                                               .columnTypes(resultSignature.getColumnTypes())
                                               .build();
    DruidQuery druidQueryMock = buildDruidQueryMock(query, resultSignature);
    // Act
    msqTaskQueryMaker = getMSQTaskQueryMaker();
    QueryResponse<Object[]> response = msqTaskQueryMaker.runQuery(druidQueryMock);
    // Assert
    String taskId = (String) Iterables.getOnlyElement(response.getResults().toList())[0];
    MSQTaskReportPayload payload = (MSQTaskReportPayload) fakeOverlordClient.taskReportAsMap(taskId)
                                                                            .get()
                                                                            .get(MSQTaskReport.REPORT_KEY)
                                                                            .getPayload();
    Assert.assertTrue(payload.getStatus().getStatus().isSuccess());
    ImmutableList<Object[]> expectedResults = ImmutableList.of(
        new Object[]{1L, ""},
        new Object[]{1L, "10.1"},
        new Object[]{1L, "2"},
        new Object[]{1L, "1"},
        new Object[]{1L, "def"},
        new Object[]{1L, "abc"}
    );
    assertResultsEquals("select cnt, dim1 from foo", expectedResults, payload.getResults().getResults());
  }

  @Test
  public void testScanQueryFailWithPolicyValidation() throws Exception
  {
    // Arrange
    policyEnforcer = new RestrictAllTablesPolicyEnforcer(null);
    RowSignature resultSignature = RowSignature.builder()
                                               .add("cnt", ColumnType.LONG)
                                               .add("dim1", ColumnType.STRING)
                                               .build();
    fieldMapping = buildFieldMapping(resultSignature);
    Query query = new Druids.ScanQueryBuilder().resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                               .eternityInterval()
                                               .dataSource(CalciteTests.DATASOURCE1)
                                               .columns(resultSignature.getColumnNames())
                                               .columnTypes(resultSignature.getColumnTypes())
                                               .build();
    DruidQuery druidQueryMock = buildDruidQueryMock(query, resultSignature);
    // Act
    msqTaskQueryMaker = getMSQTaskQueryMaker();
    QueryResponse<Object[]> response = msqTaskQueryMaker.runQuery(druidQueryMock);
    // Assert
    String taskId = (String) Iterables.getOnlyElement(response.getResults().toList())[0];
    MSQTaskReportPayload payload = (MSQTaskReportPayload) fakeOverlordClient.taskReportAsMap(taskId)
                                                                            .get()
                                                                            .get(MSQTaskReport.REPORT_KEY)
                                                                            .getPayload();
    Assert.assertTrue(payload.getStatus().getStatus().isFailure());
    MSQErrorReport errorReport = payload.getStatus().getErrorReport();
    Assert.assertTrue(errorReport.getFault().getErrorMessage().contains("Failed security validation with segment"));
  }

  @Test
  public void testScanQueryPassedPolicyValidation() throws Exception
  {
    // Arrange
    policyEnforcer = new RestrictAllTablesPolicyEnforcer(null);
    RowSignature resultSignature = RowSignature.builder()
                                               .add("cnt", ColumnType.LONG)
                                               .add("dim1", ColumnType.STRING)
                                               .build();
    fieldMapping = buildFieldMapping(resultSignature);
    RestrictedDataSource restrictedDataSource = RestrictedDataSource.create(
        TableDataSource.create(CalciteTests.DATASOURCE1),
        RowFilterPolicy.from(new EqualityFilter(
            "dim1",
            ColumnType.STRING,
            "abc",
            null
        ))
    );
    Query query = new Druids.ScanQueryBuilder().resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                               .eternityInterval()
                                               .dataSource(restrictedDataSource)
                                               .columns(resultSignature.getColumnNames())
                                               .columnTypes(resultSignature.getColumnTypes())
                                               .build();
    DruidQuery druidQueryMock = buildDruidQueryMock(query, resultSignature);
    // Act
    msqTaskQueryMaker = getMSQTaskQueryMaker();
    QueryResponse<Object[]> response = msqTaskQueryMaker.runQuery(druidQueryMock);
    // Assert
    String taskId = (String) Iterables.getOnlyElement(response.getResults().toList())[0];
    MSQTaskReportPayload payload = (MSQTaskReportPayload) fakeOverlordClient.taskReportAsMap(taskId)
                                                                            .get()
                                                                            .get(MSQTaskReport.REPORT_KEY)
                                                                            .getPayload();
    Assert.assertTrue(payload.getStatus().getStatus().isSuccess());
    ImmutableList<Object[]> expectedResults = ImmutableList.of(new Object[]{1L, "abc"});
    assertResultsEquals(
        "select cnt, dim1 from foo (with restriction)",
        expectedResults,
        payload.getResults().getResults()
    );
  }

  @Test
  public void testUnnestOnRestrictedPassedPolicyValidation() throws Exception
  {
    // Arrange
    policyEnforcer = new RestrictAllTablesPolicyEnforcer(null);
    RowSignature resultSignature = RowSignature.builder()
                                               .add("dim1", ColumnType.STRING)
                                               .add("j0.unnest", ColumnType.STRING)
                                               .build();
    fieldMapping = buildFieldMapping(resultSignature);
    UnnestDataSource unnestDataSource = UnnestDataSource.create(
        RestrictedDataSource.create(
            TableDataSource.create(CalciteTests.DATASOURCE1),
            RowFilterPolicy.from(new EqualityFilter(
                "dim1",
                ColumnType.STRING,
                "10.1",
                null
            ))
        ),
        expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
        null
    );
    Query query = new Druids.ScanQueryBuilder().resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                               .dataSource(unnestDataSource)
                                               .eternityInterval()
                                               .columns(resultSignature.getColumnNames())
                                               .columnTypes(resultSignature.getColumnTypes())
                                               .build();
    DruidQuery druidQueryMock = buildDruidQueryMock(query, resultSignature);
    // Act
    msqTaskQueryMaker = getMSQTaskQueryMaker();
    QueryResponse<Object[]> response = msqTaskQueryMaker.runQuery(druidQueryMock);
    // Assert
    String taskId = (String) Iterables.getOnlyElement(response.getResults().toList())[0];
    MSQTaskReportPayload payload = (MSQTaskReportPayload) fakeOverlordClient.taskReportAsMap(taskId)
                                                                            .get()
                                                                            .get(MSQTaskReport.REPORT_KEY)
                                                                            .getPayload();
    Assert.assertTrue(payload.getStatus().getStatus().isSuccess());
    ImmutableList<Object[]> expectedResults = ImmutableList.of(new Object[]{"10.1", "b"}, new Object[]{"10.1", "c"});
    assertResultsEquals(
        "SELECT dim1 FROM foo, UNNEST(MV_TO_ARRAY(dim3)) as unnested (d3) (with restriction)",
        expectedResults,
        payload.getResults().getResults()
    );
  }


  @Test
  public void testInlineDataSourcePassedPolicyValidation() throws Exception
  {
    // Arrange
    policyEnforcer = new RestrictAllTablesPolicyEnforcer(null);
    RowSignature resultSignature = RowSignature.builder()
                                               .add("EXPR$0", ColumnType.LONG)
                                               .build();
    fieldMapping = buildFieldMapping(resultSignature);
    InlineDataSource inlineDataSource = InlineDataSource.fromIterable(
        ImmutableList.of(new Object[]{2L}),
        resultSignature
    );
    Query query = new Druids.ScanQueryBuilder().resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                               .dataSource(inlineDataSource)
                                               .eternityInterval()
                                               .columns(resultSignature.getColumnNames())
                                               .columnTypes(resultSignature.getColumnTypes())
                                               .build();
    DruidQuery druidQueryMock = buildDruidQueryMock(query, resultSignature);
    // Act
    msqTaskQueryMaker = getMSQTaskQueryMaker();
    QueryResponse<Object[]> response = msqTaskQueryMaker.runQuery(druidQueryMock);
    // Assert
    String taskId = (String) Iterables.getOnlyElement(response.getResults().toList())[0];
    MSQTaskReportPayload payload = (MSQTaskReportPayload) fakeOverlordClient.taskReportAsMap(taskId)
                                                                            .get()
                                                                            .get(MSQTaskReport.REPORT_KEY)
                                                                            .getPayload();
    Assert.assertTrue(payload.getStatus().getStatus().isSuccess());
    ImmutableList<Object[]> expectedResults = ImmutableList.of(new Object[]{2L});
    assertResultsEquals("select 1 + 1", expectedResults, payload.getResults().getResults());
  }

  @Test
  public void testLookupDataSourcePassedPolicyValidation() throws Exception
  {
    // Arrange
    policyEnforcer = new RestrictAllTablesPolicyEnforcer(null);
    final RowSignature resultSignature = RowSignature.builder().add("v", ColumnType.STRING).build();
    fieldMapping = buildFieldMapping(resultSignature);
    Query query = new Druids.ScanQueryBuilder().resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                               .eternityInterval()
                                               .dataSource(new LookupDataSource("lookyloo"))
                                               .columns(resultSignature.getColumnNames())
                                               .columnTypes(resultSignature.getColumnTypes())
                                               .orderBy(ImmutableList.of(OrderBy.ascending("v")))
                                               .build();
    DruidQuery druidQueryMock = buildDruidQueryMock(query, resultSignature);
    // Act
    msqTaskQueryMaker = getMSQTaskQueryMaker();
    QueryResponse<Object[]> response = msqTaskQueryMaker.runQuery(druidQueryMock);
    // Assert
    String taskId = (String) Iterables.getOnlyElement(response.getResults().toList())[0];
    MSQTaskReportPayload payload = (MSQTaskReportPayload) fakeOverlordClient.taskReportAsMap(taskId)
                                                                            .get()
                                                                            .get(MSQTaskReport.REPORT_KEY)
                                                                            .getPayload();
    // Assert
    Assert.assertTrue(payload.getStatus().getStatus().isSuccess());
    ImmutableList<Object[]> expectedResults = ImmutableList.of(
        new Object[]{"mysteryvalue"},
        new Object[]{"x6"},
        new Object[]{"xa"},
        new Object[]{"xabc"}
    );
    assertResultsEquals("select v from lookyloo", expectedResults, payload.getResults().getResults());
  }

  @Test
  public void testJoinFailWithPolicyValidationOnLeftChild() throws Exception
  {
    // Arrange
    policyEnforcer = new RestrictAllTablesPolicyEnforcer(null);
    RowSignature resultSignature = RowSignature.builder()
                                               .add("dim1", ColumnType.STRING)
                                               .add("j0.a0", ColumnType.LONG)
                                               .build();
    fieldMapping = buildFieldMapping(resultSignature);
    RestrictedDataSource restrictedDataSource = RestrictedDataSource.create(
        TableDataSource.create(CalciteTests.DATASOURCE1),
        RowFilterPolicy.from(new EqualityFilter(
            "dim1",
            ColumnType.STRING,
            "abc",
            null
        ))
    );
    QueryDataSource rightChild = new QueryDataSource(new GroupByQuery.Builder().setInterval(Intervals.ETERNITY)
                                                                               .setDataSource(restrictedDataSource)
                                                                               .addAggregator(new CountAggregatorFactory(
                                                                                   "a0"))
                                                                               .setGranularity(Granularities.ALL)
                                                                               .build());
    JoinDataSource joinDataSourceLeftChildNoRestriction = JoinDataSource.create(
        TableDataSource.create(CalciteTests.DATASOURCE1),
        rightChild,
        "j0.",
        JoinConditionAnalysis.forExpression(
            "1",
            "j0.",
            ExprMacroTable.nil()
        ),
        JoinType.INNER,
        null,
        null,
        null
    );
    Query queryLeftChildNoRestriction = new Druids.ScanQueryBuilder().resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                                                     .eternityInterval()
                                                                     .dataSource(joinDataSourceLeftChildNoRestriction)
                                                                     .columns(resultSignature.getColumnNames())
                                                                     .columnTypes(resultSignature.getColumnTypes())
                                                                     .build();
    DruidQuery druidQueryMock = buildDruidQueryMock(queryLeftChildNoRestriction, resultSignature);
    // Act
    msqTaskQueryMaker = getMSQTaskQueryMaker();
    QueryResponse<Object[]> response = msqTaskQueryMaker.runQuery(druidQueryMock);
    // Assert
    String taskId = (String) Iterables.getOnlyElement(response.getResults().toList())[0];
    MSQTaskReportPayload payload = (MSQTaskReportPayload) fakeOverlordClient.taskReportAsMap(taskId)
                                                                            .get()
                                                                            .get(MSQTaskReport.REPORT_KEY)
                                                                            .getPayload();
    Assert.assertTrue(payload.getStatus().getStatus().isFailure());
    MSQErrorReport errorReport = payload.getStatus().getErrorReport();
    Assert.assertTrue(errorReport.getFault().getErrorMessage().contains("Failed security validation with segment"));
  }

  @Test
  public void testJoinFailWithPolicyValidationOnRightChild() throws Exception
  {
    // Arrange
    policyEnforcer = new RestrictAllTablesPolicyEnforcer(null);
    RowSignature resultSignature = RowSignature.builder()
                                               .add("dim1", ColumnType.STRING)
                                               .add("j0.a0", ColumnType.LONG)
                                               .build();
    fieldMapping = buildFieldMapping(resultSignature);
    RestrictedDataSource restrictedDataSource = RestrictedDataSource.create(
        TableDataSource.create(CalciteTests.DATASOURCE1),
        RowFilterPolicy.from(new EqualityFilter(
            "dim1",
            ColumnType.STRING,
            "abc",
            null
        ))
    );
    QueryDataSource rightChildNoRestriction = new QueryDataSource(new GroupByQuery.Builder().setInterval(Intervals.ETERNITY)
                                                                                            .setDataSource(CalciteTests.DATASOURCE1)
                                                                                            .addAggregator(new CountAggregatorFactory(
                                                                                                "a0"))
                                                                                            .setGranularity(
                                                                                                Granularities.ALL)
                                                                                            .build());
    JoinDataSource joinDataSourceRightChildNoRestriction = JoinDataSource.create(
        restrictedDataSource,
        rightChildNoRestriction,
        "j0.",
        JoinConditionAnalysis.forExpression(
            "1",
            "j0.",
            ExprMacroTable.nil()
        ),
        JoinType.INNER,
        null,
        null,
        null
    );
    Query queryRightChildNoRestriction = new Druids.ScanQueryBuilder().resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                                                      .eternityInterval()
                                                                      .dataSource(joinDataSourceRightChildNoRestriction)
                                                                      .columns(resultSignature.getColumnNames())
                                                                      .columnTypes(resultSignature.getColumnTypes())
                                                                      .build();
    DruidQuery druidQueryMock = buildDruidQueryMock(queryRightChildNoRestriction, resultSignature);
    // Act
    msqTaskQueryMaker = getMSQTaskQueryMaker();
    QueryResponse<Object[]> response = msqTaskQueryMaker.runQuery(druidQueryMock);
    // Assert
    String taskId = (String) Iterables.getOnlyElement(response.getResults().toList())[0];
    MSQTaskReportPayload payload = (MSQTaskReportPayload) fakeOverlordClient.taskReportAsMap(taskId)
                                                                            .get()
                                                                            .get(MSQTaskReport.REPORT_KEY)
                                                                            .getPayload();
    Assert.assertTrue(payload.getStatus().getStatus().isFailure());
    MSQErrorReport errorReport = payload.getStatus().getErrorReport();
    Assert.assertTrue(errorReport.getFault().getErrorMessage().contains("Failed security validation with segment"));
  }

  @Test
  public void testJoinPassedPolicyValidation() throws Exception
  {
    // Arrange
    policyEnforcer = new RestrictAllTablesPolicyEnforcer(null);
    RowSignature resultSignature = RowSignature.builder()
                                               .add("dim1", ColumnType.STRING)
                                               .add("j0.a0", ColumnType.LONG)
                                               .build();
    fieldMapping = buildFieldMapping(resultSignature);
    RestrictedDataSource restrictedDataSource = RestrictedDataSource.create(
        TableDataSource.create(CalciteTests.DATASOURCE1),
        RowFilterPolicy.from(new EqualityFilter(
            "dim1",
            ColumnType.STRING,
            "abc",
            null
        ))
    );
    QueryDataSource rightChild = new QueryDataSource(new GroupByQuery.Builder().setInterval(Intervals.ETERNITY)
                                                                               .setDataSource(restrictedDataSource)
                                                                               .addAggregator(new CountAggregatorFactory(
                                                                                   "a0"))
                                                                               .setGranularity(Granularities.ALL)
                                                                               .build());
    JoinDataSource joinDataSource = JoinDataSource.create(
        restrictedDataSource,
        rightChild,
        "j0.",
        JoinConditionAnalysis.forExpression(
            "1",
            "j0.",
            ExprMacroTable.nil()
        ),
        JoinType.INNER,
        null,
        null,
        null
    );
    Query query = new Druids.ScanQueryBuilder().resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                               .eternityInterval()
                                               .dataSource(joinDataSource)
                                               .columns(resultSignature.getColumnNames())
                                               .columnTypes(resultSignature.getColumnTypes())
                                               .build();
    DruidQuery druidQueryMock = buildDruidQueryMock(query, resultSignature);
    // Act
    msqTaskQueryMaker = getMSQTaskQueryMaker();
    QueryResponse<Object[]> response = msqTaskQueryMaker.runQuery(druidQueryMock);
    // Assert
    String taskId = (String) Iterables.getOnlyElement(response.getResults().toList())[0];
    MSQTaskReportPayload payload = (MSQTaskReportPayload) fakeOverlordClient.taskReportAsMap(taskId)
                                                                            .get()
                                                                            .get(MSQTaskReport.REPORT_KEY)
                                                                            .getPayload();
    Assert.assertTrue(payload.getStatus().getStatus().isSuccess());
    ImmutableList<Object[]> expectedResults = ImmutableList.of(new Object[]{"abc", 1L});
    assertResultsEquals(
        "select dim1, q.c from foo, (select count(*) c from foo) q",
        expectedResults,
        payload.getResults().getResults()
    );
  }

  private static DruidQuery buildDruidQueryMock(Query query, RowSignature resultSignature)
  {
    DruidQuery druidQueryMock = Mockito.mock(DruidQuery.class);
    when(druidQueryMock.getQuery()).thenReturn(query);
    when(druidQueryMock.getDataSource()).thenReturn(query.getDataSource());
    when(druidQueryMock.getOutputRowSignature()).thenReturn(resultSignature);
    when(druidQueryMock.getOutputRowType()).thenReturn(toRelDataType(resultSignature, JAVA_TYPE_FACTORY, false));
    return druidQueryMock;
  }

  private static List<Map.Entry<Integer, String>> buildFieldMapping(RowSignature resultSignature)
  {
    List<String> columns = resultSignature.getColumnNames();
    return ImmutableList.copyOf(IntStream.range(0, columns.size())
                                         .boxed()
                                         .collect(toMap(Function.identity(), columns::get))
                                         .entrySet());
  }

  private MSQTaskQueryMaker getMSQTaskQueryMaker()
  {
    // This can't be in setUp() because the fieldMapping are set in the test
    return new MSQTaskQueryMaker(
        ingestDestination,
        fakeOverlordClient,
        plannerContextMock,
        objectMapper,
        fieldMapping,
        terminalStageSpecFactory,
        new MSQTaskQueryKitSpecFactory(new DruidProcessingConfig())
    );
  }
}
