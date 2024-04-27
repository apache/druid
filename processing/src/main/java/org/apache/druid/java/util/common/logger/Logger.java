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

package org.apache.druid.java.util.common.logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

/**
 * A Logger for usage inside of Druid.  Provides a layer that allows for simple changes to the logging framework
 * with minimal changes to the Druid code.
 *
 * Log levels are used as an indication of urgency around the behavior that is being logged.  The intended generic
 * rubric for when to use the different logging levels is as follows.
 *
 * DEBUG: something that a developer wants to look at while actively debugging, but should not be included by default.
 *
 * INFO: a message that is useful to have when trying to retro-actively understand what happened in a running system.
 * There is often a fine line between INFO and DEBUG.  We want information from INFO logs but do not want to spam log
 * files either. One rubric to use to help determine if something should be INFO or DEBUG is how often we expect the
 * line to be logged.  If there is clarity that it will happen in a controlled manner such that it does not spam the
 * logs, then INFO is fine.  Additionally, it can be okay to log at INFO level even if there is a risk of spamming the
 * log file in the case that the log line only happens in specific "error-oriented" situations, this is because such
 * error-oriented situations are more likely to necessitate reading and understanding the logs to eliminate the error.
 * Additionally, it is perfectly acceptable and reasonable to log an exception at INFO level.
 *
 * WARN: a message that indicates something bad has happened in the system that a human should potentially investigate.
 * While it is bad and deserves investigation, it is of a nature that it should be able to wait until the next
 * "business day" for investigation instead of needing immediate attention.
 *
 * ERROR: a message that indicates that something bad has happened such that a human operator should take immediate
 * intervention to triage and resolve the issue as it runs a risk to the smooth operations of the system.  Logs at
 * the ERROR level should generally be severe enough to warrant paging someone in the middle of the night.
 *
 * Even though this is the intended rubric, it is very difficult to ensure that, e.g. all ERROR log lines are pageable
 * offenses.  As such, it is questionable whether an operator should actually ALWAYS page on every ERROR log line,
 * but as a directional target of when and how to log things, the above rubric should be used to evaluate if a log
 * line is at the correct level.
 */
public class Logger
{
  @VisibleForTesting
  static final int SEGMENTS_PER_LOG_MESSAGE = 64;

  private final org.slf4j.Logger log;
  private final boolean stackTraces;
  private final Logger noStackTraceLogger;

  public Logger(String name)
  {
    this(LoggerFactory.getLogger(name), true);
  }

  public Logger(Class clazz)
  {
    this(LoggerFactory.getLogger(clazz), true);
  }

  protected Logger(org.slf4j.Logger log, boolean stackTraces)
  {
    this.log = log;
    this.stackTraces = stackTraces;
    noStackTraceLogger = stackTraces ? new Logger(log, false) : this;
  }

  protected org.slf4j.Logger getSlf4jLogger()
  {
    return log;
  }

  @SuppressWarnings("unused")
  @Override
  public String toString()
  {
    return StringUtils.format("Logger{name=[%s], class[%s]}", log.getName(), log.getClass());
  }

  /**
   * Returns a copy of this Logger that does not log exception stack traces, unless the log level is DEBUG or lower.
   * Useful for writing code like: {@code log.noStackTrace().warn(e, "Something happened.");}
   */
  public Logger noStackTrace()
  {
    return noStackTraceLogger;
  }

  public void trace(String message, Object... formatArgs)
  {
    if (log.isTraceEnabled()) {
      log.trace(StringUtils.nonStrictFormat(message, formatArgs));
    }
  }

  public void trace(Marker marker, String message, Object... formatArgs)
  {
    if (log.isTraceEnabled()) {
      log.trace(marker, StringUtils.nonStrictFormat(message, formatArgs));
    }
  }

  public void debug(String message, Object... formatArgs)
  {
    if (log.isDebugEnabled()) {
      log.debug(StringUtils.nonStrictFormat(message, formatArgs));
    }
  }

  public void debug(Marker marker, String message, Object... formatArgs)
  {
    if (log.isDebugEnabled()) {
      log.debug(marker, StringUtils.nonStrictFormat(message, formatArgs));
    }
  }
  public void debug(Throwable t, String message, Object... formatArgs)
  {
    if (log.isDebugEnabled()) {
      logException(log::debug, t, StringUtils.nonStrictFormat(message, formatArgs));
    }
  }

  public void info(String message, Object... formatArgs)
  {
    if (log.isInfoEnabled()) {
      log.info(StringUtils.nonStrictFormat(message, formatArgs));
    }
  }

  public void info(Marker marker, String message, Object... formatArgs)
  {
    if (log.isInfoEnabled()) {
      log.info(marker, StringUtils.nonStrictFormat(message, formatArgs));
    }
  }

  public void info(Throwable t, String message, Object... formatArgs)
  {
    if (log.isInfoEnabled()) {
      logException(log::info, t, StringUtils.nonStrictFormat(message, formatArgs));
    }
  }

  /**
   * Protect against assuming slf4j convention. use `warn(Throwable t, String message, Object... formatArgs)` instead
   *
   * @param message The string message
   * @param t       The Throwable to log
   */
  @Deprecated
  public void warn(String message, Throwable t)
  {
    warn(t, message);
  }

  public void warn(String message, Object... formatArgs)
  {
    log.warn(StringUtils.nonStrictFormat(message, formatArgs));
  }

  public void warn(Marker marker, String message, Object... formatArgs)
  {
    log.warn(marker, StringUtils.nonStrictFormat(message, formatArgs));
  }

  public void warn(Throwable t, String message, Object... formatArgs)
  {
    logException(log::warn, t, StringUtils.nonStrictFormat(message, formatArgs));
  }

  public void error(String message, Object... formatArgs)
  {
    log.error(StringUtils.nonStrictFormat(message, formatArgs));
  }

  public void error(Marker marker, String message, Object... formatArgs)
  {
    log.error(marker, StringUtils.nonStrictFormat(message, formatArgs));
  }

  /**
   * Protect against assuming slf4j convention. use `error(Throwable t, String message, Object... formatArgs)` instead
   *
   * @param message The string message
   * @param t       The Throwable to log
   */
  @Deprecated
  public void error(String message, Throwable t)
  {
    error(t, message);
  }

  public void error(Throwable t, String message, Object... formatArgs)
  {
    logException(log::error, t, StringUtils.nonStrictFormat(message, formatArgs));
  }

  public void debugSegments(@Nullable final Collection<DataSegment> segments, @Nullable String preamble)
  {
    if (log.isDebugEnabled()) {
      logSegments(this::debug, segments, preamble);
    }
  }

  public void infoSegments(@Nullable final Collection<DataSegment> segments, @Nullable String preamble)
  {
    if (log.isInfoEnabled()) {
      logSegments(this::info, segments, preamble);
    }
  }

  public void infoSegmentIds(@Nullable final Stream<SegmentId> segments, @Nullable String preamble)
  {
    if (log.isInfoEnabled()) {
      logSegmentIds(this::info, segments, preamble);
    }
  }

  public void warnSegments(@Nullable final Collection<DataSegment> segments, @Nullable String preamble)
  {
    if (log.isWarnEnabled()) {
      logSegments(this::warn, segments, preamble);
    }
  }

  public void errorSegments(@Nullable final Collection<DataSegment> segments, @Nullable String preamble)
  {
    logSegments(this::error, segments, preamble);
  }

  public boolean isTraceEnabled()
  {
    return log.isTraceEnabled();
  }

  public boolean isDebugEnabled()
  {
    return log.isDebugEnabled();
  }

  public boolean isInfoEnabled()
  {
    return log.isInfoEnabled();
  }

  private void logException(BiConsumer<String, Throwable> fn, Throwable t, String message)
  {
    if (stackTraces || log.isDebugEnabled()) {
      fn.accept(message, t);
    } else {
      if (message.isEmpty()) {
        fn.accept(t.toString(), null);
      } else {
        fn.accept(StringUtils.nonStrictFormat("%s (%s)", message, t.toString()), null);
      }
    }
  }

  public String getName()
  {
    return this.log.getName();
  }

  /**
   * Logs all the segment ids you could ever want, {@link #SEGMENTS_PER_LOG_MESSAGE} at a time, as a comma separated
   * list.
   */
  @VisibleForTesting
  static void logSegments(
      Logger.LogFunction logger,
      @Nullable final Collection<DataSegment> segments,
      @Nullable String preamble
  )
  {
    if (segments == null || segments.isEmpty()) {
      return;
    }
    logSegmentIds(logger, segments.stream().map(DataSegment::getId), preamble);
  }

  /**
   * Logs all the segment ids you could ever want, {@link #SEGMENTS_PER_LOG_MESSAGE} at a time, as a comma separated
   * list.
   */
  @VisibleForTesting
  static void logSegmentIds(
      Logger.LogFunction logger,
      @Nullable final Stream<SegmentId> stream,
      @Nullable String preamble
  )
  {
    Preconditions.checkNotNull(preamble);
    if (stream == null) {
      return;
    }
    final Iterator<SegmentId> iterator = stream.iterator();
    if (!iterator.hasNext()) {
      return;
    }
    final String logFormat = preamble + ": %s";

    int counter = 0;
    StringBuilder sb = null;
    while (iterator.hasNext()) {
      SegmentId nextId = iterator.next();
      if (counter == 0) {
        // use segmentId string length of first as estimate for total size of builder for this batch
        sb = new StringBuilder(SEGMENTS_PER_LOG_MESSAGE * (2 + nextId.safeUpperLimitOfStringSize())).append("[");
      }
      sb.append(nextId);
      if (++counter < SEGMENTS_PER_LOG_MESSAGE && iterator.hasNext()) {
        sb.append(", ");
      }
      counter = counter % SEGMENTS_PER_LOG_MESSAGE;
      if (counter == 0) {
        // flush
        sb.append("]");
        logger.log(logFormat, sb.toString());
      }
    }

    // check for stragglers
    if (counter > 0) {
      sb.append("]");
      logger.log(logFormat, sb.toString());
    }
  }

  @FunctionalInterface
  public interface LogFunction
  {
    void log(String msg, Object... format);
  }
}
