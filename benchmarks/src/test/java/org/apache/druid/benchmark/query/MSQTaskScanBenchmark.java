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

package org.apache.druid.benchmark.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.msq.sql.MSQTaskSqlEngine;
import org.apache.druid.msq.test.MSQTestOverlordServiceClient;
import org.apache.druid.msq.test.StandardMSQComponentSupplier;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.QueryTestBuilder;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
import org.apache.druid.sql.calcite.TempDirProducer;
import org.openjdk.jmh.infra.Blackhole;

import java.util.List;
import java.util.Map;

/**
 * {@link MSQScanBenchmarkBase} on the MSQ task engine.
 */
@SqlTestFrameworkConfig.ComponentSupplier(MSQTaskScanBenchmark.ComponentSupplier.class)
public class MSQTaskScanBenchmark extends MSQScanBenchmarkBase
{
  @Override
  protected MSQTaskReportPayload runQueryAndGetReport(
      final String sql,
      final Map<String, Object> context,
      final Blackhole blackhole
  )
  {
    // Without a results-extraction step, the task engine's only result row is the task ID.
    final List<Object[]> taskIdRows = new QueryTestBuilder(createCalciteTestConfig())
        .skipVectorize(true)
        .queryContext(context)
        .sql(sql)
        .results()
        .results;

    final String taskId = taskIdRows.get(0)[0].toString();
    final MSQTestOverlordServiceClient overlordClient =
        (MSQTestOverlordServiceClient) ((MSQTaskSqlEngine) queryFramework().engine()).overlordClient();
    final MSQTaskReportPayload payload =
        (MSQTaskReportPayload) overlordClient.getReportForTask(taskId).get(MSQTaskReport.REPORT_KEY).getPayload();
    blackhole.consume(payload.getResults());
    return payload;
  }

  protected static class ComponentSupplier extends StandardMSQComponentSupplier
  {
    public ComponentSupplier(final TempDirProducer tempFolderProducer)
    {
      super(tempFolderProducer);
    }

    @Override
    public SpecificSegmentsQuerySegmentWalker addSegmentsToWalker(
        final SpecificSegmentsQuerySegmentWalker walker,
        final ObjectMapper jsonMapper
    )
    {
      return addSegments(super.addSegmentsToWalker(walker, jsonMapper));
    }
  }
}
