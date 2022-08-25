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

package org.apache.druid.msq.indexing.report;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.indexing.common.TaskReport;

@JsonTypeName(MSQTaskReport.REPORT_KEY)
public class MSQTaskReport implements TaskReport
{
  public static final String REPORT_KEY = "multiStageQuery";

  private final String taskId;
  private final MSQTaskReportPayload payload;

  @JsonCreator
  public MSQTaskReport(
      @JsonProperty("taskId") final String taskId,
      @JsonProperty("payload") final MSQTaskReportPayload payload
  )
  {
    this.taskId = taskId;
    this.payload = payload;
  }

  @Override
  @JsonProperty
  public String getTaskId()
  {
    return taskId;
  }

  @Override
  public String getReportKey()
  {
    return REPORT_KEY;
  }

  @Override
  @JsonProperty
  public MSQTaskReportPayload getPayload()
  {
    return payload;
  }
}
