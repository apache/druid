package com.metamx.druid.http;

import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.MutableDateTime;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;

/**
 */
public class FileRequestLogger implements RequestLogger
{
  private final ScheduledExecutorService exec;
  private final File baseDir;

  private final Object lock = new Object();

  private volatile DateTime currentDay;
  private volatile FileWriter fileWriter;

  public FileRequestLogger(ScheduledExecutorService exec, File baseDir)
  {
    this.exec = exec;
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

      fileWriter = new FileWriter(new File(baseDir, currentDay.toString("yyyy-MM-dd'.log'")), true);
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
                  Closeables.closeQuietly(fileWriter);
                  fileWriter = new FileWriter(new File(baseDir, currentDay.toString()), true);
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
      Closeables.closeQuietly(fileWriter);
    }
  }

  @Override
  public void log(RequestLogLine requestLogLine) throws Exception
  {
    synchronized (lock) {
      fileWriter.write(
          String.format("%s%n", requestLogLine.getLine())
      );
      fileWriter.flush();
    }
  }
}
