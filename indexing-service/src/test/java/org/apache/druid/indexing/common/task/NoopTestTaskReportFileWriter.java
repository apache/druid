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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.indexer.report.TaskReportFileWriter;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

public class NoopTestTaskReportFileWriter implements TaskReportFileWriter
{
  @Override
  public void write(String id, TaskReport.ReportMap reports)
  {
  }

  @Override
  public OutputStream openReportOutputStream(String taskId)
  {
    // Stream to nowhere.
    return new ByteArrayOutputStream();
  }

  @Override
  public void setObjectMapper(ObjectMapper objectMapper)
  {

  }
}
