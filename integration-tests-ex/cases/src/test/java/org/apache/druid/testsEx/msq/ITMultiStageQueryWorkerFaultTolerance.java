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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.sql.SqlTaskStatus;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.testing.clients.CoordinatorResourceTestClient;
import org.apache.druid.testing.utils.DataLoaderHelper;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.apache.druid.testing.utils.MsqTestQueryHelper;
import org.apache.druid.testsEx.categories.MultiStageQueryWithMM;
import org.apache.druid.testsEx.config.DruidTestRunner;
import org.apache.druid.testsEx.utils.DruidClusterAdminClient;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * As we need to kill the PID of the launched task, these tests should be run with middle manager only.
 */
@RunWith(DruidTestRunner.class)
@Category(MultiStageQueryWithMM.class)
public class ITMultiStageQueryWorkerFaultTolerance
{
  private static final Logger LOG = new Logger(ITMultiStageQueryWorkerFaultTolerance.class);
  @Inject
  private MsqTestQueryHelper msqHelper;

  @Inject
  private DataLoaderHelper dataLoaderHelper;

  @Inject
  private CoordinatorResourceTestClient coordinatorClient;

  @Inject
  private DruidClusterAdminClient druidClusterAdminClient;

  private static final String QUERY_FILE = "/multi-stage-query/wikipedia_msq_select_query_ha.json";

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
            + "    '{\"type\":\"local\",\"files\":[\"/resources/data/batch_index/json/wikipedia_index_data1.json\",\"/resources/data/batch_index/json/wikipedia_index_data1.json\"]}',\n"
            + "    '{\"type\":\"json\"}',\n"
            + "    '[{\"type\":\"string\",\"name\":\"timestamp\"},{\"type\":\"string\",\"name\":\"isRobot\"},{\"type\":\"string\",\"name\":\"diffUrl\"},{\"type\":\"long\",\"name\":\"added\"},{\"type\":\"string\",\"name\":\"countryIsoCode\"},{\"type\":\"string\",\"name\":\"regionName\"},{\"type\":\"string\",\"name\":\"channel\"},{\"type\":\"string\",\"name\":\"flags\"},{\"type\":\"long\",\"name\":\"delta\"},{\"type\":\"string\",\"name\":\"isUnpatrolled\"},{\"type\":\"string\",\"name\":\"isNew\"},{\"type\":\"double\",\"name\":\"deltaBucket\"},{\"type\":\"string\",\"name\":\"isMinor\"},{\"type\":\"string\",\"name\":\"isAnonymous\"},{\"type\":\"long\",\"name\":\"deleted\"},{\"type\":\"string\",\"name\":\"cityName\"},{\"type\":\"long\",\"name\":\"metroCode\"},{\"type\":\"string\",\"name\":\"namespace\"},{\"type\":\"string\",\"name\":\"comment\"},{\"type\":\"string\",\"name\":\"page\"},{\"type\":\"long\",\"name\":\"commentLength\"},{\"type\":\"string\",\"name\":\"countryName\"},{\"type\":\"string\",\"name\":\"user\"},{\"type\":\"string\",\"name\":\"regionIsoCode\"}]'\n"
            + "  )\n"
            + ")\n"
            + "PARTITIONED BY DAY\n"
            + "CLUSTERED BY \"__time\"",
            datasource
        );

    // Submit the task and wait for the datasource to get loaded
    SqlTaskStatus sqlTaskStatus = msqHelper.submitMsqTaskSuccesfully(
        queryLocal,
        ImmutableMap.of(
            MultiStageQueryContext.CTX_FAULT_TOLERANCE,
            "true",
            MultiStageQueryContext.CTX_MAX_NUM_TASKS,
            3
        )
    );

    if (sqlTaskStatus.getState().isFailure()) {
      Assert.fail(StringUtils.format(
          "Unable to start the task successfully.\nPossible exception: %s",
          sqlTaskStatus.getError()
      ));
    }


    String taskIdToKill = sqlTaskStatus.getTaskId() + "-worker1_0";
    killTaskAbruptly(taskIdToKill);

    msqHelper.pollTaskIdForSuccess(sqlTaskStatus.getTaskId());
    dataLoaderHelper.waitUntilDatasourceIsReady(datasource);

    msqHelper.testQueriesFromFile(QUERY_FILE, datasource);
  }

  private void killTaskAbruptly(String taskIdToKill)
  {

    String command = "jps -mlv | grep -i peon | grep -i " + taskIdToKill + " |awk  '{print  $1}'";

    ITRetryUtil.retryUntil(() -> {

      Pair<String, String> stdOut = druidClusterAdminClient.runCommandInMiddleManagerContainer("/bin/bash", "-c",
                                                                                               command
      );
      LOG.info(StringUtils.format(
          "command %s \nstdout: %s\nstderr: %s",
          command,
          stdOut.lhs,
          stdOut.rhs
      ));
      if (stdOut.rhs != null && stdOut.rhs.length() != 0) {
        throw new ISE("Bad command");
      }
      String pidToKill = stdOut.lhs.trim();
      if (pidToKill.length() != 0) {
        LOG.info("Found PID to kill %s", pidToKill);
        // kill worker after 5 seconds
        Thread.sleep(5000);
        LOG.info("Killing pid %s", pidToKill);
        druidClusterAdminClient.runCommandInMiddleManagerContainer(
            "/bin/bash",
            "-c",
            "kill -9 " + pidToKill
        );
        return true;
      } else {
        return false;
      }
    }, true, 6000, 50, StringUtils.format("Figuring out PID for task[%s] to kill abruptly", taskIdToKill));


  }
}
