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

package org.apache.druid.testsEx.msq;

import com.google.api.client.util.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.msq.indexing.report.MSQResultsReport;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.msq.sql.SqlTaskStatus;
import org.apache.druid.storage.local.LocalFileExportStorageProvider;
import org.apache.druid.testing.clients.CoordinatorResourceTestClient;
import org.apache.druid.testing.utils.DataLoaderHelper;
import org.apache.druid.testing.utils.MsqTestQueryHelper;
import org.apache.druid.testsEx.categories.MultiStageQuery;
import org.apache.druid.testsEx.config.DruidTestRunner;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@RunWith(DruidTestRunner.class)
@Category(MultiStageQuery.class)
public class ITMultiStageQuery
{
  @Inject
  private MsqTestQueryHelper msqHelper;

  @Inject
  private DataLoaderHelper dataLoaderHelper;

  @Inject
  private CoordinatorResourceTestClient coordinatorClient;

  private static final String QUERY_FILE = "/multi-stage-query/wikipedia_msq_select_query1.json";

  @Test
  public void testMsqIngestionAndQuerying() throws Exception
  {
    String datasource = "dst";

    // Clear up the datasource from the previous runs
    coordinatorClient.unloadSegmentsForDataSource(datasource);

    String queryLocal =
        StringUtils.format(
            "INSERT INTO %s\n"
            + "SELECT\n"
            + "  TIME_PARSE(\"timestamp\") AS __time,\n"
            + "  isRobot,\n"
            + "  diffUrl,\n"
            + "  added,\n"
            + "  countryIsoCode,\n"
            + "  regionName,\n"
            + "  channel,\n"
            + "  flags,\n"
            + "  delta,\n"
            + "  isUnpatrolled,\n"
            + "  isNew,\n"
            + "  deltaBucket,\n"
            + "  isMinor,\n"
            + "  isAnonymous,\n"
            + "  deleted,\n"
            + "  cityName,\n"
            + "  metroCode,\n"
            + "  namespace,\n"
            + "  comment,\n"
            + "  page,\n"
            + "  commentLength,\n"
            + "  countryName,\n"
            + "  user,\n"
            + "  regionIsoCode\n"
            + "FROM TABLE(\n"
            + "  EXTERN(\n"
            + "    '{\"type\":\"local\",\"files\":[\"/resources/data/batch_index/json/wikipedia_index_data1.json\"]}',\n"
            + "    '{\"type\":\"json\"}',\n"
            + "    '[{\"type\":\"string\",\"name\":\"timestamp\"},{\"type\":\"string\",\"name\":\"isRobot\"},{\"type\":\"string\",\"name\":\"diffUrl\"},{\"type\":\"long\",\"name\":\"added\"},{\"type\":\"string\",\"name\":\"countryIsoCode\"},{\"type\":\"string\",\"name\":\"regionName\"},{\"type\":\"string\",\"name\":\"channel\"},{\"type\":\"string\",\"name\":\"flags\"},{\"type\":\"long\",\"name\":\"delta\"},{\"type\":\"string\",\"name\":\"isUnpatrolled\"},{\"type\":\"string\",\"name\":\"isNew\"},{\"type\":\"double\",\"name\":\"deltaBucket\"},{\"type\":\"string\",\"name\":\"isMinor\"},{\"type\":\"string\",\"name\":\"isAnonymous\"},{\"type\":\"long\",\"name\":\"deleted\"},{\"type\":\"string\",\"name\":\"cityName\"},{\"type\":\"long\",\"name\":\"metroCode\"},{\"type\":\"string\",\"name\":\"namespace\"},{\"type\":\"string\",\"name\":\"comment\"},{\"type\":\"string\",\"name\":\"page\"},{\"type\":\"long\",\"name\":\"commentLength\"},{\"type\":\"string\",\"name\":\"countryName\"},{\"type\":\"string\",\"name\":\"user\"},{\"type\":\"string\",\"name\":\"regionIsoCode\"}]'\n"
            + "  )\n"
            + ")\n"
            + "PARTITIONED BY DAY\n",
            datasource
        );

    // Submit the task and wait for the datasource to get loaded
    SqlTaskStatus sqlTaskStatus = msqHelper.submitMsqTaskSuccesfully(queryLocal);

    if (sqlTaskStatus.getState().isFailure()) {
      Assert.fail(StringUtils.format(
          "Unable to start the task successfully.\nPossible exception: %s",
          sqlTaskStatus.getError()
      ));
    }

    msqHelper.pollTaskIdForSuccess(sqlTaskStatus.getTaskId());
    dataLoaderHelper.waitUntilDatasourceIsReady(datasource);

    msqHelper.testQueriesFromFile(QUERY_FILE, datasource);
  }

  @Test
  @Ignore("localfiles() is disabled")
  public void testMsqIngestionAndQueryingWithLocalFn() throws Exception
  {
    String datasource = "dst";

    // Clear up the datasource from the previous runs
    coordinatorClient.unloadSegmentsForDataSource(datasource);

    String queryLocal =
        StringUtils.format(
            "INSERT INTO %s\n"
            + "SELECT\n"
            + "  TIME_PARSE(\"timestamp\") AS __time,\n"
            + "  isRobot,\n"
            + "  diffUrl,\n"
            + "  added,\n"
            + "  countryIsoCode,\n"
            + "  regionName,\n"
            + "  channel,\n"
            + "  flags,\n"
            + "  delta,\n"
            + "  isUnpatrolled,\n"
            + "  isNew,\n"
            + "  deltaBucket,\n"
            + "  isMinor,\n"
            + "  isAnonymous,\n"
            + "  deleted,\n"
            + "  cityName,\n"
            + "  metroCode,\n"
            + "  namespace,\n"
            + "  comment,\n"
            + "  page,\n"
            + "  commentLength,\n"
            + "  countryName,\n"
            + "  user,\n"
            + "  regionIsoCode\n"
            + "FROM TABLE(\n"
            + "  LOCALFILES(\n"
            + "    files => ARRAY['/resources/data/batch_index/json/wikipedia_index_data1.json'],\n"
            + "    format => 'json'\n"
            + "  ))\n"
            + "  (\"timestamp\" VARCHAR, isRobot VARCHAR, diffUrl VARCHAR, added BIGINT, countryIsoCode VARCHAR, regionName VARCHAR,\n"
            + "   channel VARCHAR, flags VARCHAR, delta BIGINT, isUnpatrolled VARCHAR, isNew VARCHAR, deltaBucket DOUBLE,\n"
            + "   isMinor VARCHAR, isAnonymous VARCHAR, deleted BIGINT, cityName VARCHAR, metroCode BIGINT, namespace VARCHAR,\n"
            + "   comment VARCHAR, page VARCHAR, commentLength BIGINT, countryName VARCHAR, \"user\" VARCHAR, regionIsoCode VARCHAR)\n"
            + "PARTITIONED BY DAY\n",
            datasource
        );

    // Submit the task and wait for the datasource to get loaded
    SqlTaskStatus sqlTaskStatus = msqHelper.submitMsqTaskSuccesfully(queryLocal);

    if (sqlTaskStatus.getState().isFailure()) {
      Assert.fail(StringUtils.format(
          "Unable to start the task successfully.\nPossible exception: %s",
          sqlTaskStatus.getError()
      ));
    }

    msqHelper.pollTaskIdForSuccess(sqlTaskStatus.getTaskId());
    dataLoaderHelper.waitUntilDatasourceIsReady(datasource);

    msqHelper.testQueriesFromFile(QUERY_FILE, datasource);
  }

  @Test
  public void testExport() throws Exception
  {
    String exportQuery =
        StringUtils.format(
            "INSERT INTO extern(%s(exportPath => '%s'))\n"
            + "AS CSV\n"
            + "SELECT page, added, delta\n"
            + "FROM TABLE(\n"
            + "  EXTERN(\n"
            + "    '{\"type\":\"local\",\"files\":[\"/resources/data/batch_index/json/wikipedia_index_data1.json\"]}',\n"
            + "    '{\"type\":\"json\"}',\n"
            + "    '[{\"type\":\"string\",\"name\":\"timestamp\"},{\"type\":\"string\",\"name\":\"isRobot\"},{\"type\":\"string\",\"name\":\"diffUrl\"},{\"type\":\"long\",\"name\":\"added\"},{\"type\":\"string\",\"name\":\"countryIsoCode\"},{\"type\":\"string\",\"name\":\"regionName\"},{\"type\":\"string\",\"name\":\"channel\"},{\"type\":\"string\",\"name\":\"flags\"},{\"type\":\"long\",\"name\":\"delta\"},{\"type\":\"string\",\"name\":\"isUnpatrolled\"},{\"type\":\"string\",\"name\":\"isNew\"},{\"type\":\"double\",\"name\":\"deltaBucket\"},{\"type\":\"string\",\"name\":\"isMinor\"},{\"type\":\"string\",\"name\":\"isAnonymous\"},{\"type\":\"long\",\"name\":\"deleted\"},{\"type\":\"string\",\"name\":\"cityName\"},{\"type\":\"long\",\"name\":\"metroCode\"},{\"type\":\"string\",\"name\":\"namespace\"},{\"type\":\"string\",\"name\":\"comment\"},{\"type\":\"string\",\"name\":\"page\"},{\"type\":\"long\",\"name\":\"commentLength\"},{\"type\":\"string\",\"name\":\"countryName\"},{\"type\":\"string\",\"name\":\"user\"},{\"type\":\"string\",\"name\":\"regionIsoCode\"}]'\n"
            + "  )\n"
            + ")\n",
            LocalFileExportStorageProvider.TYPE_NAME, "/shared/export/"
        );

    SqlTaskStatus exportTask = msqHelper.submitMsqTaskSuccesfully(exportQuery);

    msqHelper.pollTaskIdForSuccess(exportTask.getTaskId());

    if (exportTask.getState().isFailure()) {
      Assert.fail(StringUtils.format(
          "Unable to start the task successfully.\nPossible exception: %s",
          exportTask.getError()
      ));
    }

    String resultQuery = StringUtils.format(
        "SELECT page, delta, added\n"
        + "  FROM TABLE(\n"
        + "    EXTERN(\n"
        + "      '{\"type\":\"local\",\"baseDir\":\"/shared/export/\",\"filter\":\"*.csv\"}',\n"
        + "      '{\"type\":\"csv\",\"findColumnsFromHeader\":true}'\n"
        + "    )\n"
        + "  ) EXTEND (\"added\" BIGINT, \"delta\" BIGINT, \"page\" VARCHAR)\n"
        + "   WHERE delta != 0\n"
        + "   ORDER BY page");

    SqlTaskStatus resultTaskStatus = msqHelper.submitMsqTaskSuccesfully(resultQuery);

    msqHelper.pollTaskIdForSuccess(resultTaskStatus.getTaskId());

    Map<String, MSQTaskReport> statusReport = msqHelper.fetchStatusReports(resultTaskStatus.getTaskId());
    MSQTaskReport taskReport = statusReport.get(MSQTaskReport.REPORT_KEY);
    if (taskReport == null) {
      throw new ISE("Unable to fetch the status report for the task [%]", resultTaskStatus.getTaskId());
    }
    MSQTaskReportPayload taskReportPayload = Preconditions.checkNotNull(
        taskReport.getPayload(),
        "payload"
    );
    MSQResultsReport resultsReport = Preconditions.checkNotNull(
        taskReportPayload.getResults(),
        "Results report for the task id is empty"
    );

    Yielder<Object[]> yielder = resultsReport.getResultYielder();
    List<List<Object>> actualResults = new ArrayList<>();

    while (!yielder.isDone()) {
      Object[] row = yielder.get();
      actualResults.add(Arrays.asList(row));
      yielder = yielder.next(null);
    }

    ImmutableList<ImmutableList<Object>> expectedResults = ImmutableList.of(
        ImmutableList.of("Cherno Alpha", 111, 123),
        ImmutableList.of("Gypsy Danger", -143, 57),
        ImmutableList.of("Striker Eureka", 330, 459)
    );

    Assert.assertEquals(
        expectedResults,
        actualResults
    );
  }
}
