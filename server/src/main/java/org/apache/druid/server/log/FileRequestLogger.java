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

package org.apache.druid.server.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.RequestLogLine;
import org.apache.druid.utils.CloseableUtils;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.MutableDateTime;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Request logger implementation that logs query requests and writes them to a file.
 */
public class FileRequestLogger implements RequestLogger
{
  private static final Logger log = new Logger(FileRequestLogger.class);

  private final ObjectMapper objectMapper;
  private final ScheduledExecutorService exec;
  private final File baseDir;
  private final DateTimeFormatter filePattern;
  private final Duration durationToRetain;
  private final Duration rollPeriod;
  private final Object lock = new Object();

  private DateTime currentPeriod;
  private OutputStreamWriter fileWriter;

  public FileRequestLogger(
      ObjectMapper objectMapper,
      ScheduledExecutorService exec,
      File baseDir,
      String filePattern,
      Duration durationToRetain,
      Duration rollPeriod
  )
  {
    this.exec = exec;
    this.objectMapper = objectMapper;
    this.baseDir = baseDir;
    this.filePattern = DateTimeFormat.forPattern(filePattern);
    this.durationToRetain = durationToRetain;
    this.rollPeriod = rollPeriod;
    Preconditions.checkArgument(
        this.durationToRetain == null || this.durationToRetain.compareTo(Duration.standardHours(1)) >= 0,
        "request logs retention period must be atleast PT1H"
    );
    Preconditions.checkArgument(
        this.rollPeriod == null || this.rollPeriod.compareTo(Duration.standardHours(1)) >= 0,
        "request logs roll period must be atleast PT1H"
    );
  }

  @LifecycleStart
  @Override
  public void start()
  {
    try {
      FileUtils.mkdirp(baseDir);

      MutableDateTime mutableDateTime = DateTimes.nowUtc().toMutableDateTime(ISOChronology.getInstanceUTC());
      mutableDateTime.setMinuteOfHour(0);
      mutableDateTime.setSecondOfMinute(0);
      mutableDateTime.setMillisOfSecond(0);
      if (rollPeriod.compareTo(Duration.standardDays(1)) >= 0) {
        mutableDateTime.setHourOfDay(0);
      }
      synchronized (lock) {
        currentPeriod = mutableDateTime.toDateTime(ISOChronology.getInstanceUTC());

        fileWriter = getFileWriter();
      }
      long nextPeriodMillis = currentPeriod.plus(rollPeriod).getMillis();
      Duration initialDelay = new Duration(nextPeriodMillis - System.currentTimeMillis());

      ScheduledExecutors.scheduleWithFixedDelay(
          exec,
          initialDelay,
          rollPeriod,
          new Callable<>()
          {
            @Override
            public ScheduledExecutors.Signal call()
            {
              try {
                synchronized (lock) {
                  currentPeriod = currentPeriod.plus(rollPeriod);

                  CloseableUtils.closeAndSuppressExceptions(
                      fileWriter,
                      e -> log.warn("Could not close log file for %s. Creating new log file anyway.", currentPeriod)
                  );

                  fileWriter = getFileWriter();
                }
              }
              catch (Exception e) {
                throw new RuntimeException(e);
              }

              return ScheduledExecutors.Signal.REPEAT;
            }
          }
      );
      if (durationToRetain != null) {
        ScheduledExecutors.scheduleWithFixedDelay(
            exec,
            new Duration(0),
            Duration.standardDays(1),
            new Runnable()
            {
              @Override
              public void run()
              {
                try {
                  long timestamp = System.currentTimeMillis() - durationToRetain.getMillis();
                  removeFilesOlderThan(timestamp);
                }
                catch (Exception ex) {
                  log.error(ex, "Failed to clean-up the request logs");
                }
              }
            }
        );
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void removeFilesOlderThan(long timestamp) throws IOException
  {
    File[] files = baseDir.listFiles(f -> f.lastModified() < timestamp);
    if (files != null) {
      for (File file : files) {
        log.info("Deleting request log [%s].", file.getAbsolutePath());
        org.apache.commons.io.FileUtils.forceDelete(file);
        if (Thread.currentThread().isInterrupted()) {
          throw new IOException(
              new InterruptedException("Thread interrupted. Couldn't delete all tasklogs.")
          );
        }
      }
    }
  }

  private OutputStreamWriter getFileWriter() throws FileNotFoundException
  {
    return new OutputStreamWriter(
        new FileOutputStream(new File(baseDir, filePattern.print(currentPeriod)), true),
        StandardCharsets.UTF_8
    );
  }

  @LifecycleStop
  @Override
  public void stop()
  {
    synchronized (lock) {
      CloseableUtils.closeAndWrapExceptions(fileWriter);
    }
  }

  @Override
  public void logNativeQuery(RequestLogLine requestLogLine) throws IOException
  {
    logToFile(requestLogLine.getNativeQueryLine(objectMapper));
  }

  @Override
  public void logSqlQuery(RequestLogLine requestLogLine) throws IOException
  {
    logToFile(requestLogLine.getSqlQueryLine(objectMapper));
  }

  private void logToFile(final String message) throws IOException
  {
    synchronized (lock) {
      fileWriter.write(message);
      fileWriter.write("\n");
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
