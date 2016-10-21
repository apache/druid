/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;

import io.druid.java.util.common.concurrent.ScheduledExecutors;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.server.RequestLogLine;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.MutableDateTime;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
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

  private volatile DateTime currentDay;
  private volatile OutputStreamWriter fileWriter;

  public FileRequestLogger(ObjectMapper objectMapper, ScheduledExecutorService exec, File baseDir)
  {
    this.exec = exec;
    this.objectMapper = objectMapper;
    this.baseDir = baseDir;
  }

  @LifecycleStart
  public void start()
  {
    try {
      baseDir.mkdirs();

      MutableDateTime mutableDateTime = new DateTime().toMutableDateTime();
      mutableDateTime.setMillisOfDay(0);
      currentDay = mutableDateTime.toDateTime();

      fileWriter = new OutputStreamWriter(
          new FileOutputStream(new File(baseDir, currentDay.toString("yyyy-MM-dd'.log'")), true),
          Charsets.UTF_8
      );
      long nextDay = currentDay.plusDays(1).getMillis();
      Duration delay = new Duration(nextDay - new DateTime().getMillis());

      ScheduledExecutors.scheduleWithFixedDelay(
          exec,
          delay,
          Duration.standardDays(1),
          new Callable<ScheduledExecutors.Signal>()
          {
            @Override
            public ScheduledExecutors.Signal call()
            {
              currentDay = currentDay.plusDays(1);

              try {
                synchronized (lock) {
                  CloseQuietly.close(fileWriter);
                  fileWriter = new OutputStreamWriter(
                      new FileOutputStream(new File(baseDir, currentDay.toString()), true),
                      Charsets.UTF_8
                  );
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

  @LifecycleStop
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
          String.format("%s%n", requestLogLine.getLine(objectMapper))
      );
      fileWriter.flush();
    }
  }
}
