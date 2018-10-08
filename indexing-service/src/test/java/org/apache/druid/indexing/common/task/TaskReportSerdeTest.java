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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexer.IngestionState;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReport;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReportData;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class TaskReportSerdeTest
{
  private final ObjectMapper jsonMapper;

  public TaskReportSerdeTest()
  {
    TestUtils testUtils = new TestUtils();
    jsonMapper = testUtils.getTestObjectMapper();
  }

  @Test
  public void testSerde() throws Exception
  {
    IngestionStatsAndErrorsTaskReport report1 = new IngestionStatsAndErrorsTaskReport(
        "testID",
        new IngestionStatsAndErrorsTaskReportData(
            IngestionState.BUILD_SEGMENTS,
            ImmutableMap.of(
                "hello", "world"
            ),
            ImmutableMap.of(
                "number", 1234
            ),
            "an error message"
        )
    );
    String report1serialized = jsonMapper.writeValueAsString(report1);
    IngestionStatsAndErrorsTaskReport report2 = jsonMapper.readValue(
        report1serialized,
        IngestionStatsAndErrorsTaskReport.class
    );
    Assert.assertEquals(report1, report2);


    Map<String, TaskReport> reportMap1 = TaskReport.buildTaskReports(report1);
    String reportMapSerialized = jsonMapper.writeValueAsString(reportMap1);
    Map<String, TaskReport> reportMap2 = jsonMapper.readValue(
        reportMapSerialized,
        new TypeReference<Map<String, TaskReport>>()
        {
        }
    );
    Assert.assertEquals(reportMap1, reportMap2);
  }
}
