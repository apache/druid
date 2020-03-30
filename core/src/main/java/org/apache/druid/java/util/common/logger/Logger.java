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

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

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

  public void debug(String message, Object... formatArgs)
  {
    if (log.isDebugEnabled()) {
      log.debug(StringUtils.nonStrictFormat(message, formatArgs));
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

  public void warn(Throwable t, String message, Object... formatArgs)
  {
    logException(log::warn, t, StringUtils.nonStrictFormat(message, formatArgs));
  }

  public void error(String message, Object... formatArgs)
  {
    log.error(StringUtils.nonStrictFormat(message, formatArgs));
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

  public void assertionError(String message, Object... formatArgs)
  {
    log.error("ASSERTION_ERROR: " + message, formatArgs);
  }

  public void wtf(String message, Object... formatArgs)
  {
    error(message, formatArgs);
  }

  public void wtf(Throwable t, String message, Object... formatArgs)
  {
    error(t, message, formatArgs);
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
