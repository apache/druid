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

package org.apache.druid.sql.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.server.log.AbstractFileRequestLogger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.concurrent.ScheduledExecutorService;

public class FileSqlRequestLogger extends AbstractFileRequestLogger implements SqlRequestLogger
{
  private final ObjectMapper objectMapper;

  public FileSqlRequestLogger(ObjectMapper objectMapper, ScheduledExecutorService exec, File baseDir)
  {
    super(exec, baseDir);
    this.objectMapper = objectMapper;
  }

  @Override
  protected OutputStreamWriter getFileWriter() throws FileNotFoundException
  {
    return new OutputStreamWriter(
        new FileOutputStream(new File(baseDir, currentDay.toString("'sql.'yyyy-MM-dd'.log'")), true),
        Charsets.UTF_8
    );
  }

  @Override
  @LifecycleStart
  public void start()
  {
    super.start();
  }

  @Override
  public void log(SqlRequestLogLine sqlRequestLogLine) throws IOException
  {
    String message = "# " + String.valueOf(sqlRequestLogLine.getTimestamp()) + " "
                     + sqlRequestLogLine.getRemoteAddr() + " "
                     + objectMapper.writeValueAsString(sqlRequestLogLine.getQueryStats()) + "\n"
                     + sqlRequestLogLine.getSql() + "\n";

    logToFile(message);
  }

  @Override
  @LifecycleStop
  public void stop()
  {
    super.stop();
  }

  @Override
  public String toString()
  {
    return "FileSqlRequestLogger{" +
           "baseDir=" + baseDir +
           '}';
  }
}

