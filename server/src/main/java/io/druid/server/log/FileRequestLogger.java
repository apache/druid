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

package io.druid.server.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.concurrent.ScheduledExecutors;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.server.RequestLogLine;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.MutableDateTime;
import org.joda.time.chrono.ISOChronology;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;

/**
 */
public class FileRequestLogger implements RequestLogger
{
  private final ObjectMapper objectMapper;
  private final ScheduledExecutorService exec;
  private final File baseDir;

  private final Object lock = new Object();

  private DateTime currentDay;
  private OutputStreamWriter fileWriter;

  public FileRequestLogger(ObjectMapper objectMapper, ScheduledExecutorService exec, File baseDir)
  {
    this.exec = exec;
    this.objectMapper = objectMapper;
    this.baseDir = baseDir;
  }

  @LifecycleStart
  @Override
  public void start() throws Exception
  {
    try {
      baseDir.mkdirs();

      MutableDateTime mutableDateTime = DateTimes.nowUtc().toMutableDateTime(ISOChronology.getInstanceUTC());
      mutableDateTime.setMillisOfDay(0);
      synchronized (lock) {
        currentDay = mutableDateTime.toDateTime(ISOChronology.getInstanceUTC());

        fileWriter = getFileWriter();
      }
      long nextDay = currentDay.plusDays(1).getMillis();
      Duration initialDelay = new Duration(nextDay - System.currentTimeMillis());

      ScheduledExecutors.scheduleWithFixedDelay(
          exec,
          initialDelay,
          Duration.standardDays(1),
          new Callable<ScheduledExecutors.Signal>()
          {
            @Override
            public ScheduledExecutors.Signal call()
            {
              try {
                synchronized (lock) {
                  currentDay = currentDay.plusDays(1);
                  CloseQuietly.close(fileWriter);
                  fileWriter = getFileWriter();
                }
              }
              catch (Exception e) {
                Throwables.propagate(e);
              }

              return ScheduledExecutors.Signal.REPEAT;
            }
          }
      );
    }
    catch (IOException e) {
      Throwables.propagate(e);
    }
  }

  private OutputStreamWriter getFileWriter() throws FileNotFoundException
  {
    return new OutputStreamWriter(
        new FileOutputStream(new File(baseDir, currentDay.toString("yyyy-MM-dd'.log'")), true),
        StandardCharsets.UTF_8
    );
  }

  @LifecycleStop
  @Override
  public void stop()
  {
    synchronized (lock) {
      CloseQuietly.close(fileWriter);
    }
  }

  @Override
  public void log(RequestLogLine requestLogLine) throws IOException
  {
    synchronized (lock) {
      fileWriter.write(
          StringUtils.format("%s%n", requestLogLine.getLine(objectMapper))
      );
      fileWriter.flush();
    }
  }

  @Override
  public String toString()
  {
    return "FileRequestLogger{" +
           "baseDir=" + baseDir +
           '}';
  }
}
