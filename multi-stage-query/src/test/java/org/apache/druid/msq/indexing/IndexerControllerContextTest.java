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

package org.apache.druid.msq.indexing;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import org.apache.druid.client.indexing.IndexingService;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.AbstractTask;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.msq.exec.MemoryIntrospector;
import org.apache.druid.msq.guice.MultiStageQuery;
import org.apache.druid.msq.indexing.destination.TaskReportMSQDestination;
import org.apache.druid.msq.input.InputSpecSlicerProvider;
import org.apache.druid.msq.kernel.controller.ControllerQueryKernelConfig;
import org.apache.druid.msq.sql.MSQTaskSqlEngine;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.LegacySegmentSpec;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.apache.druid.storage.StorageConnector;
import org.apache.druid.storage.StorageConnectorProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.Map;
import java.util.Set;

public class IndexerControllerContextTest
{
  @Test
  public void testMakeTaskContextCopiesOnlyWorkerStartupKeys()
  {
    final Map<String, Object> queryContext = ImmutableMap.of(
        QueryContextParameters.QUERY_ID.getName(), "query-id",
        QueryContextParameters.SQL_QUERY_ID.getName(), "sql-query-id",
        QueryContextParameters.MAX_THREADS.getName(), 2,
        Tasks.STORE_EMPTY_COLUMNS_KEY, false,
        QueryContextParameters.FINALIZE_AGGREGATIONS.getName(), false,
        QueryContextParameters.ARRAY_INGEST_MODE.getName(), "array"
    );

    final LegacyMSQSpec querySpec = makeQuerySpec(queryContext);

    final ControllerQueryKernelConfig queryKernelConfig =
        ControllerQueryKernelConfig.builder()
                                   .maxRetainedPartitionSketchBytes(1)
                                   .destination(querySpec.getDestination())
                                   .build();

    final Map<String, Object> taskContext =
        IndexerControllerContext.makeTaskContext(querySpec, queryKernelConfig, ImmutableMap.of());

    // Keys that workers read from the task context at startup.
    Assertions.assertEquals("query-id", taskContext.get(QueryContextParameters.QUERY_ID.getName()));
    Assertions.assertEquals("sql-query-id", taskContext.get(QueryContextParameters.SQL_QUERY_ID.getName()));
    Assertions.assertEquals(2, taskContext.get(QueryContextParameters.MAX_THREADS.getName()));
    Assertions.assertEquals(false, taskContext.get(Tasks.STORE_EMPTY_COLUMNS_KEY));
    Assertions.assertEquals(
        TaskReportMSQDestination.instance().toSelectDestination().getName(),
        taskContext.get(QueryContextParameters.SELECT_DESTINATION.getName())
    );

    // Other query context keys reach workers through the work order, not the task context.
    Assertions.assertFalse(taskContext.containsKey(QueryContextParameters.FINALIZE_AGGREGATIONS.getName()));
    Assertions.assertFalse(taskContext.containsKey(QueryContextParameters.ARRAY_INGEST_MODE.getName()));
  }

  @Test
  public void test_makeMetricBuilder_startsFromTaskMetricBuilder()
  {
    final Map<String, Object> tags = Map.of("tag", "value");
    final MSQControllerTask task = makeTask(
        Map.of(
            QueryContextParameters.QUERY_ID.getName(), "query",
            QueryContextParameters.SQL_QUERY_ID.getName(), "sqlQuery"
        ),
        Map.of(DruidMetrics.TAGS, tags)
    );

    final ServiceMetricEvent event = makeContext(task).makeMetricBuilder()
                                                      .setMetric("query/time", 1)
                                                      .build(ImmutableMap.of());

    Assertions.assertEquals(
        ImmutableMap.builder()
                    .put(DruidMetrics.TASK_ID, "controller")
                    .put(DruidMetrics.TASK_TYPE, MSQControllerTask.TYPE)
                    .put(DruidMetrics.DATASOURCE, MSQControllerTask.DUMMY_DATASOURCE_FOR_SELECT)
                    .put(DruidMetrics.TASK_INGESTION_MODE, AbstractTask.IngestionMode.NONE)
                    .put(DruidMetrics.TAGS, tags)
                    .put(DruidMetrics.GROUP_ID, "controller")
                    .put(DruidMetrics.ID, "query")
                    .put(DruidMetrics.QUERY_ID, "query")
                    .put(QueryContextParameters.SQL_QUERY_ID.getName(), "sqlQuery")
                    .put(DruidMetrics.TYPE, "msq")
                    .put(DruidMetrics.ENGINE, MSQTaskSqlEngine.NAME)
                    .build(),
        event.getUserDims()
    );
  }

  @Test
  public void test_makeMetricBuilder_noQueryIdInContext()
  {
    final MSQControllerTask task = makeTask(
        Map.of(QueryContextParameters.SQL_QUERY_ID.getName(), "sqlQuery"),
        Map.of()
    );

    final ServiceMetricEvent event = makeContext(task).makeMetricBuilder()
                                                      .setMetric("query/time", 1)
                                                      .build(ImmutableMap.of());

    Assertions.assertFalse(event.getUserDims().containsKey(DruidMetrics.ID));
    Assertions.assertFalse(event.getUserDims().containsKey(DruidMetrics.QUERY_ID));
    Assertions.assertEquals("sqlQuery", event.getUserDims().get(QueryContextParameters.SQL_QUERY_ID.getName()));
  }

  @Test
  public void test_makeMetricBuilder_queryDataSourceOverridesTaskDataSource()
  {
    final MSQControllerTask task = makeTask(Map.of(), Map.of());

    final ServiceMetricEvent event = makeContext(task).makeMetricBuilder()
                                                      .setDimension(DruidMetrics.DATASOURCE, "queryDataSource")
                                                      .setMetric("query/time", 1)
                                                      .build(ImmutableMap.of());

    Assertions.assertEquals("queryDataSource", event.getUserDims().get(DruidMetrics.DATASOURCE));
    Assertions.assertEquals("controller", event.getUserDims().get(DruidMetrics.TASK_ID));
  }

  private static LegacyMSQSpec makeQuerySpec(final Map<String, Object> queryContext)
  {
    return LegacyMSQSpec.builder()
                        .query(
                            Druids.newScanQueryBuilder()
                                  .dataSource(new TableDataSource("foo"))
                                  .intervals(new LegacySegmentSpec(Intervals.ETERNITY))
                                  .columns("dim1")
                                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                  .build()
                        )
                        .queryContext(QueryContext.of(queryContext))
                        .columnMappings(ColumnMappings.identity(RowSignature.builder().add("dim1", ColumnType.STRING).build()))
                        .destination(TaskReportMSQDestination.instance())
                        .tuningConfig(MSQTuningConfig.defaultConfig())
                        .build();
  }

  private static MSQControllerTask makeTask(
      final Map<String, Object> queryContext,
      final Map<String, Object> taskContext
  )
  {
    return new MSQControllerTask("controller", makeQuerySpec(queryContext), null, null, null, null, null, taskContext);
  }

  private static IndexerControllerContext makeContext(final MSQControllerTask task)
  {
    final StorageConnectorProvider storageConnectorProvider = Mockito.mock(StorageConnectorProvider.class);
    Mockito.when(storageConnectorProvider.createStorageConnector(ArgumentMatchers.any()))
           .thenReturn(Mockito.mock(StorageConnector.class));

    final Injector injector = Mockito.mock(Injector.class);
    Mockito.when(injector.getInstance(MemoryIntrospector.class)).thenReturn(Mockito.mock(MemoryIntrospector.class));
    Mockito.when(injector.getInstance(Key.get(StorageConnectorProvider.class, MultiStageQuery.class)))
           .thenReturn(storageConnectorProvider);
    Mockito.when(injector.getInstance(Key.get(new TypeLiteral<Set<InputSpecSlicerProvider>>() {}, IndexingService.class)))
           .thenReturn(Set.of());

    return new IndexerControllerContext(task, Mockito.mock(TaskToolbox.class), injector, null, null);
  }
}
