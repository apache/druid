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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.dart.controller.DartControllerRegistry;
import org.apache.druid.msq.dart.controller.QueryInfoAndReport;
import org.apache.druid.msq.dart.guice.DartModules;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.msq.test.DartComponentSupplier;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.QueryTestBuilder;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
import org.apache.druid.sql.calcite.TempDirProducer;
import org.openjdk.jmh.infra.Blackhole;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * {@link MSQScanBenchmarkBase} on the Dart engine.
 */
@SqlTestFrameworkConfig.ComponentSupplier(DartScanBenchmark.ComponentSupplier.class)
public class DartScanBenchmark extends MSQScanBenchmarkBase
{
  @Override
  protected MSQTaskReportPayload runQueryAndGetReport(
      final String sql,
      final Map<String, Object> context,
      final Blackhole blackhole
  )
  {
    final String sqlQueryId = UUID.randomUUID().toString();
    final Map<String, Object> dartContext = new HashMap<>(context);
    dartContext.put(QueryContextParameters.SQL_QUERY_ID.getName(), sqlQueryId);

    final List<Object[]> rows = new QueryTestBuilder(createCalciteTestConfig())
        .skipVectorize(true)
        .queryContext(dartContext)
        .sql(sql)
        .results()
        .results;
    blackhole.consume(rows);

    // The controller files its complete report as it deregisters, which can happen just after the results are
    // returned. Until then, the registry returns the running query's live report.
    final DartControllerRegistry registry = queryFramework().injector().getInstance(DartControllerRegistry.class);
    final long deadline = System.currentTimeMillis() + 10_000;
    while (System.currentTimeMillis() < deadline) {
      final QueryInfoAndReport details = registry.getQueryDetailsBySqlQueryId(sqlQueryId);
      if (details != null && details.getReportMap() != null) {
        final MSQTaskReportPayload payload =
            (MSQTaskReportPayload) details.getReportMap().get(MSQTaskReport.REPORT_KEY).getPayload();
        if (payload.getStatus().getStatus().isComplete()) {
          return payload;
        }
      }
      try {
        Thread.sleep(1);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }
    throw new ISE("No report for sqlQueryId[%s]", sqlQueryId);
  }

  protected static class ComponentSupplier extends DartComponentSupplier
  {
    public ComponentSupplier(final TempDirProducer tempFolderProducer)
    {
      super(tempFolderProducer);
    }

    @Override
    public void gatherProperties(final Properties properties)
    {
      super.gatherProperties(properties);
      // Retain the report of each finished query, so its counters can be read.
      properties.put(DartModules.DART_PROPERTY_BASE + ".controller.maxRetainedReportCount", "10");
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
