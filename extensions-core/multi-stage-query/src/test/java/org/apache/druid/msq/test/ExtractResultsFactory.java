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

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.sql.calcite.QueryTestBuilder;
import org.apache.druid.sql.calcite.QueryTestRunner;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * {@link QueryTestRunner.QueryRunStep} that extracts the results from the reports so that they can be used in the
 * subsequent verification steps
 */
public class ExtractResultsFactory implements QueryTestRunner.QueryRunStepFactory
{
  private final Supplier<MSQTestOverlordServiceClient> overlordClientSupplier;

  /**
   * @param overlordClientSupplier Supplier to the overlord client which contains the reports for the test run. A
   *                               supplier is required because the step is instantiated when the overlord client might
   *                               not be instantiated however when we fetch the results, the overlord client must be
   *                               instantiated by the query framework
   */
  public ExtractResultsFactory(Supplier<MSQTestOverlordServiceClient> overlordClientSupplier)
  {
    this.overlordClientSupplier = overlordClientSupplier;
  }

  @Override
  public QueryTestRunner.QueryRunStep make(QueryTestBuilder builder, QueryTestRunner.BaseExecuteQuery execStep)
  {
    return new QueryTestRunner.BaseExecuteQuery(builder)
    {
      final List<QueryTestRunner.QueryResults> extractedResults = new ArrayList<>();

      final MSQTestOverlordServiceClient overlordClient = overlordClientSupplier.get();

      @Override
      public void run()
      {
        for (QueryTestRunner.QueryResults results : execStep.results()) {
          List<Object[]> queryResults = results.results;
          if (queryResults == null) {
            extractedResults.add(results);
            return;
          }
          // For a single run, only a single query results containing a single row must be fetched, since UNION is not
          // currently supported by MSQ
          Assert.assertEquals(
              "Found multiple rows, cannot extract the actual results from the reports",
              1,
              queryResults.size()
          );
          Object[] row = queryResults.get(0);
          Assert.assertEquals(
              "Found multiple taskIds, cannot extract the actual results from the reports",
              1,
              row.length
          );
          String taskId = row[0].toString();
          MSQTaskReportPayload payload = (MSQTaskReportPayload) overlordClient.getReportForTask(taskId)
                                                                              .get(MSQTaskReport.REPORT_KEY)
                                                                              .getPayload();
          if (payload.getStatus().getStatus().isFailure()) {
            throw new ISE(
                "Query task [%s] failed due to %s",
                taskId,
                payload.getStatus().getErrorReport().toString()
            );
          }

          if (!payload.getStatus().getStatus().isComplete()) {
            throw new ISE("Query task [%s] should have finished", taskId);
          }
          final List<Object[]> resultRows = MSQTestBase.getRows(payload.getResults());
          if (resultRows == null) {
            throw new ISE("Results report not present in the task's report payload");
          }
          extractedResults.add(results.withResults(resultRows));
        }
      }

      @Override
      public List<QueryTestRunner.QueryResults> results()
      {
        return extractedResults;
      }
    };
  }
}
