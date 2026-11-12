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
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.msq.indexing.destination.TaskReportMSQDestination;
import org.apache.druid.msq.kernel.controller.ControllerQueryKernelConfig;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.LegacySegmentSpec;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

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

    final LegacyMSQSpec querySpec =
        LegacyMSQSpec.builder()
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
}
