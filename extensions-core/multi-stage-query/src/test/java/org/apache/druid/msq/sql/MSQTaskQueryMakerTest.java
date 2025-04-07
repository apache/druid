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
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.testing.fieldbinder.Bind;
import com.google.inject.testing.fieldbinder.BoundFieldModule;
import com.google.inject.util.Modules;
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
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.msq.exec.DataServerQueryHandlerFactory;
import org.apache.druid.msq.indexing.MSQControllerTask;
import org.apache.druid.msq.indexing.MSQSpec;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.msq.indexing.destination.TaskReportMSQDestination;
import org.apache.druid.msq.indexing.error.MSQErrorReport;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.msq.querykit.DataSegmentProvider;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.msq.test.MSQTestOverlordServiceClient;
import org.apache.druid.msq.test.MSQTestTaskActionClient;
import org.apache.druid.query.Druids;
import org.apache.druid.query.ForwardingQueryProcessingPool;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.RestrictedDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnnestDataSource;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.policy.NoopPolicyEnforcer;
import org.apache.druid.query.policy.PolicyEnforcer;
import org.apache.druid.query.policy.RestrictAllTablesPolicyEnforcer;
import org.apache.druid.query.policy.RowFilterPolicy;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.LookylooModule;
import org.apache.druid.sql.calcite.util.TestDataBuilder;
import org.apache.druid.timeline.SegmentId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.function.Supplier;

import static org.apache.druid.sql.calcite.BaseCalciteQueryTest.assertResultsEquals;
import static org.apache.druid.sql.calcite.BaseCalciteQueryTest.expressionVirtualColumn;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.when;

public class MSQTaskQueryMakerTest
{
  private static final Closer CLOSER = Closer.create();

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
  private IndexIO indexIO;
  @Bind
  private QueryProcessingPool queryProcessingPool;
  @Bind
  @Nullable
  private DataServerQueryHandlerFactory dataServerQueryHandlerFactory;
  @Bind(lazy = true)
  private PolicyEnforcer policyEnforcer; // lazy so we can set it in the test

  private MSQTestOverlordServiceClient overlordClient;

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
      return ReferenceCountingResourceHolder.fromCloseable(walker.getSegment(segmentId));
    });

    objectMapper = TestHelper.makeJsonMapper();
    indexIO = new IndexIO(objectMapper, ColumnConfig.DEFAULT);
    queryProcessingPool = new ForwardingQueryProcessingPool(Execs.singleThreaded("Test-runner-processing-pool"));
    policyEnforcer = NoopPolicyEnforcer.instance();
    Module defaultModule = Modules.combine(
        binder -> binder.bind(Key.get(ObjectMapper.class, Json.class)).toInstance(new DefaultObjectMapper()),
        new ExpressionModule(),
        new DruidGuiceExtensions(),
        new LifecycleModule(),
        new ConfigModule(),
        new SegmentWranglerModule(),
        new LookylooModule()
    );
    Injector injector = Guice.createInjector(defaultModule, BoundFieldModule.of(this));
    DruidSecondaryModule.setupJackson(injector, objectMapper);

    overlordClient = new MSQTestOverlordServiceClient(
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
    ScanQuery query = new Druids.ScanQueryBuilder()
        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
        .eternityInterval()
        .dataSource(CalciteTests.DATASOURCE1)
        .columns(ImmutableList.of("cnt", "dim1"))
        .columnTypes(ColumnType.LONG, ColumnType.STRING)
        .build();
    RowSignature resultSignature = RowSignature.builder()
                                               .add("cnt", ColumnType.LONG)
                                               .add("dim1", ColumnType.STRING)
                                               .build();
    String controllerTaskId = "scan_query";
    MSQControllerTask controllerTask = new MSQControllerTask(
        controllerTaskId,
        MSQSpec.builder()
               .query(query)
               .columnMappings(ColumnMappings.identity(resultSignature))
               .tuningConfig(MSQTuningConfig.defaultConfig())
               .destination(TaskReportMSQDestination.INSTANCE)
               .build(),
        "select cnt, dim1 from foo",
        null,
        null,
        null,
        null,
        null,
        null
    );
    // Act
    overlordClient.runTask(controllerTaskId, controllerTask).get();
    MSQTaskReportPayload payload = (MSQTaskReportPayload) overlordClient.getReportForTask(controllerTaskId)
                                                                        .get(MSQTaskReport.REPORT_KEY)
                                                                        .getPayload();
    // Assert
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
    ScanQuery query = new Druids.ScanQueryBuilder()
        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
        .eternityInterval()
        .dataSource(CalciteTests.DATASOURCE1)
        .columns(ImmutableList.of("cnt", "dim1"))
        .columnTypes(ColumnType.LONG, ColumnType.STRING)
        .build();
    RowSignature resultSignature = RowSignature.builder()
                                               .add("cnt", ColumnType.LONG)
                                               .add("dim1", ColumnType.STRING)
                                               .build();
    String controllerTaskId = "scan_query";
    MSQControllerTask controllerTask = buildMSQControllerTask(
        controllerTaskId,
        query,
        resultSignature,
        "select cnt, dim1 from foo"
    );
    // Act
    overlordClient.runTask(controllerTaskId, controllerTask).get();
    MSQTaskReportPayload payload = (MSQTaskReportPayload) overlordClient.getReportForTask(controllerTaskId)
                                                                        .get(MSQTaskReport.REPORT_KEY)
                                                                        .getPayload();
    // Assert
    Assert.assertTrue(payload.getStatus().getStatus().isFailure());
    MSQErrorReport errorReport = payload.getStatus().getErrorReport();
    Assert.assertTrue(errorReport.getFault().getErrorMessage().contains("Failed security validation"));
  }

  @Test
  public void testScanQueryPassedPolicyValidation() throws Exception
  {
    // Arrange
    policyEnforcer = new RestrictAllTablesPolicyEnforcer(null);
    RestrictedDataSource restrictedDataSource = RestrictedDataSource.create(
        TableDataSource.create(CalciteTests.DATASOURCE1),
        RowFilterPolicy.from(new EqualityFilter(
            "dim1",
            ColumnType.STRING,
            "abc",
            null
        ))
    );
    ScanQuery query = new Druids.ScanQueryBuilder()
        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
        .eternityInterval()
        .dataSource(restrictedDataSource)
        .columns(ImmutableList.of("cnt", "dim1"))
        .columnTypes(ColumnType.LONG, ColumnType.STRING)
        .build();
    RowSignature resultSignature = RowSignature.builder()
                                               .add("cnt", ColumnType.LONG)
                                               .add("dim1", ColumnType.STRING)
                                               .build();
    String controllerTaskId = "scan_query";
    MSQControllerTask controllerTask = buildMSQControllerTask(
        controllerTaskId,
        query,
        resultSignature,
        "select cnt, dim1 from foo (with restriction)"
    );
    // Act
    overlordClient.runTask(controllerTaskId, controllerTask).get();
    MSQTaskReportPayload payload = (MSQTaskReportPayload) overlordClient.getReportForTask(controllerTaskId)
                                                                        .get(MSQTaskReport.REPORT_KEY)
                                                                        .getPayload();
    // Assert
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
    ScanQuery query = Druids.newScanQueryBuilder()
                            .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                            .dataSource(unnestDataSource)
                            .eternityInterval()
                            .columns("dim1", "j0.unnest")
                            .columnTypes(ColumnType.STRING, ColumnType.STRING)
                            .build();
    RowSignature resultSignature = RowSignature.builder()
                                               .add("dim1", ColumnType.STRING)
                                               .add("j0.unnest", ColumnType.STRING)
                                               .build();

    String controllerTaskId = "scan_query";
    MSQControllerTask controllerTask = buildMSQControllerTask(
        controllerTaskId,
        query,
        resultSignature,
        "SELECT dim1 FROM foo, UNNEST(MV_TO_ARRAY(dim3)) as unnested (d3)"
    );
    // Act
    overlordClient.runTask(controllerTaskId, controllerTask).get();
    MSQTaskReportPayload payload = (MSQTaskReportPayload) overlordClient.getReportForTask(controllerTaskId)
                                                                        .get(MSQTaskReport.REPORT_KEY)
                                                                        .getPayload();
    // Assert
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
    InlineDataSource inlineDataSource = InlineDataSource.fromIterable(
        ImmutableList.of(new Object[]{2L}),
        resultSignature
    );
    ScanQuery query = Druids.newScanQueryBuilder()
                            .dataSource(inlineDataSource)
                            .eternityInterval()
                            .columns("EXPR$0")
                            .columnTypes(ColumnType.LONG)
                            .build();
    String controllerTaskId = "scan_query";
    MSQControllerTask controllerTask = buildMSQControllerTask(controllerTaskId, query, resultSignature, "select 1 + 1");
    // Act
    overlordClient.runTask(controllerTaskId, controllerTask).get();
    MSQTaskReportPayload payload = (MSQTaskReportPayload) overlordClient.getReportForTask(controllerTaskId)
                                                                        .get(MSQTaskReport.REPORT_KEY)
                                                                        .getPayload();
    // Assert
    Assert.assertTrue(payload.getStatus().getStatus().isSuccess());
    ImmutableList<Object[]> expectedResults = ImmutableList.of(new Object[]{2L});
    assertResultsEquals("select 1 + 1", expectedResults, payload.getResults().getResults());
  }

  @Test
  public void testLookupDataSourcePassedPolicyValidation() throws Exception
  {
    // Arrange
    policyEnforcer = new RestrictAllTablesPolicyEnforcer(null);
    final RowSignature rowSignature = RowSignature.builder().add("v", ColumnType.STRING).build();
    ScanQuery query = Druids.newScanQueryBuilder()
                            .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                            .eternityInterval()
                            .dataSource(new LookupDataSource("lookyloo"))
                            .columns(ImmutableList.of("v"))
                            .columnTypes(ColumnType.STRING)
                            .orderBy(ImmutableList.of(OrderBy.ascending("v")))
                            .build();
    String controllerTaskId = "scan_query";
    MSQControllerTask controllerTask = buildMSQControllerTask(
        controllerTaskId,
        query,
        rowSignature,
        "select v from lookyloo"
    );
    // Act
    overlordClient.runTask(controllerTaskId, controllerTask).get();
    MSQTaskReportPayload payload = (MSQTaskReportPayload) overlordClient.getReportForTask(controllerTaskId)
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

  private static MSQControllerTask buildMSQControllerTask(
      String controllerTaskId,
      Query<?> query,
      RowSignature resultSignature,
      String sql
  )
  {
    return new MSQControllerTask(
        controllerTaskId,
        MSQSpec.builder()
               .query(query)
               .columnMappings(ColumnMappings.identity(resultSignature))
               .tuningConfig(MSQTuningConfig.defaultConfig())
               .destination(TaskReportMSQDestination.INSTANCE)
               .build(),
        sql,
        null,
        null,
        null,
        null,
        null,
        null
    );
  }
}
