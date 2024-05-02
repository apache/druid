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

package org.apache.druid.indexing.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.indexer.report.SingleFileTaskReportFileWriter;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.indexer.report.TaskReportFileWriter;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

public class MultipleFileTaskReportFileWriter implements TaskReportFileWriter
{
  private static final Logger log = new Logger(MultipleFileTaskReportFileWriter.class);

  private final Map<String, File> taskReportFiles = new HashMap<>();

  private ObjectMapper objectMapper;

  @Override
  public void write(String taskId, TaskReport.ReportMap reports)
  {
    try (final OutputStream outputStream = openReportOutputStream(taskId)) {
      SingleFileTaskReportFileWriter.writeReportToStream(objectMapper, outputStream, reports);
    }
    catch (Exception e) {
      log.error(e, "Encountered exception in write().");
    }
  }

  @Override
  public OutputStream openReportOutputStream(String taskId) throws IOException
  {
    final File reportsFile = taskReportFiles.get(taskId);
    if (reportsFile == null) {
      throw new ISE("Could not find report file for task[%s]", taskId);
    }

    final File reportsFileParent = reportsFile.getParentFile();
    if (reportsFileParent != null) {
      FileUtils.mkdirp(reportsFileParent);
    }

    return Files.newOutputStream(reportsFile.toPath());
  }

  @Override
  public void setObjectMapper(ObjectMapper objectMapper)
  {
    this.objectMapper = objectMapper;
  }

  public void add(String taskId, File reportsFile)
  {
    taskReportFiles.put(taskId, reportsFile);
  }

  public void delete(String taskId)
  {
    taskReportFiles.remove(taskId);
  }
}
