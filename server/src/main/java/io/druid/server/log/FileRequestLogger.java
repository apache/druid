/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.server.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.guava.CloseQuietly;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
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
