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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.msq.indexing.report.MSQStagesReport;
import org.apache.druid.msq.indexing.report.MSQStatusReport;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.clients.MsqTestClient;
import org.apache.druid.testing.utils.MsqTestQueryHelper;
import org.apache.druid.testsEx.categories.BatchIndex;
import org.apache.druid.testsEx.config.DruidTestRunner;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayDeque;
import java.util.Map;

@RunWith(DruidTestRunner.class)
@Category(BatchIndex.class)
public class ITMultiStageQuery
{
  @Inject
  private MsqTestQueryHelper msqHelper;

  @Inject
  private MsqTestClient msqClient;

  @Inject
  private IntegrationTestingConfig config;
  @Inject

  private ObjectMapper jsonMapper;

  @Test
  public void test() throws Exception
  {
    String query =
        "INSERT INTO dst SELECT *\n"
        + "FROM TABLE(extern(\n"
        + "   '{\n"
        + "     \"type\": \"inline\",\n"
        + "     \"data\": \"a,b,1\\nc,d,2\\n\"\n"
        + "    }',\n"
        + "  '{\n"
        + "    \"type\": \"csv\",\n"
        + "    \"columns\": [\"x\",\"y\",\"z\"],\n"
        + "    \"listDelimiter\": null,\n"
        + "    \"findColumnsFromHeader\": false,\n"
        + "    \"skipHeaderRows\": 0\n"
        + "   }',\n"
        + "   '[\n"
        + "     {\"name\": \"x\", \"type\": \"STRING\"},\n"
        + "     {\"name\": \"y\", \"type\": \"STRING\"},\n"
        + "     {\"name\": \"z\", \"type\": \"LONG\"}\n"
        + "   ]'\n"
        + "))\n"
        + "PARTITIONED BY ALL TIME";
    String taskId = msqHelper.submitMsqTask(query);
    msqHelper.pollTaskIdForCompletion(taskId, 0);
    Map<String, MSQTaskReport> reports = msqHelper.fetchStatusReports(taskId);
    int x = 5;
    x += 1;
  }

  @Test
  public void scratchPad() throws JsonProcessingException
  {
    MSQTaskReport msqTaskReport = new MSQTaskReport(
        "test-id",
        new MSQTaskReportPayload(
            new MSQStatusReport(
                TaskState.RUNNING,
                null,
                new ArrayDeque<>(),
                DateTime.now(),
                1
            ),
            new MSQStagesReport(ImmutableList.of()),
            null,
            null
        )
    );
    String serialized = jsonMapper.writeValueAsString(msqTaskReport);
    MSQTaskReport deserializEd = jsonMapper.readValue(serialized, MSQTaskReport.class);
    int x = 6;
    x += 1;
  }
}

