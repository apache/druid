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
import org.apache.commons.io.FileUtils;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.File;
import java.util.Map;

public class SingleFileTaskReportFileWriter implements TaskReportFileWriter
{
  private static final Logger log = new Logger(SingleFileTaskReportFileWriter.class);

  private final File reportsFile;
  private ObjectMapper objectMapper;

  public SingleFileTaskReportFileWriter(File reportsFile)
  {
    this.reportsFile = reportsFile;
  }

  @Override
  public void write(String taskId, Map<String, TaskReport> reports)
  {
    try {
      final File reportsFileParent = reportsFile.getParentFile();
      if (reportsFileParent != null) {
        FileUtils.forceMkdir(reportsFileParent);
      }
      objectMapper.writeValue(reportsFile, reports);
    }
    catch (Exception e) {
      log.error(e, "Encountered exception in write().");
    }
  }


  @Override
  public void setObjectMapper(ObjectMapper objectMapper)
  {
    this.objectMapper = objectMapper;
  }
}
