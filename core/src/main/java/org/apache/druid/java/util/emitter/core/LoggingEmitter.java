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

package org.apache.druid.java.util.emitter.core;

/**
 */

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 */
public class LoggingEmitter implements Emitter
{
  private final Logger log;
  private final Level level;
  private final ObjectMapper jsonMapper;

  private final AtomicBoolean started = new AtomicBoolean(false);

  public LoggingEmitter(LoggingEmitterConfig config, ObjectMapper jsonMapper)
  {
    this(new Logger(config.getLoggerClass()), Level.toLevel(config.getLogLevel()), jsonMapper);
  }

  public LoggingEmitter(Logger log, Level level, ObjectMapper jsonMapper)
  {
    this.log = log;
    this.level = level;
    this.jsonMapper = jsonMapper;
  }

  @Override
  @LifecycleStart
  public void start()
  {
    final boolean alreadyStarted = started.getAndSet(true);
    if (!alreadyStarted) {
      final String message = "Start: started [%s]";
      switch (level) {
        case TRACE:
          if (log.isTraceEnabled()) {
            log.trace(message, started.get());
          }
          break;
        case DEBUG:
          if (log.isDebugEnabled()) {
            log.debug(message, started.get());
          }
          break;
        case INFO:
          if (log.isInfoEnabled()) {
            log.info(message, started.get());
          }
          break;
        case WARN:
          log.warn(message, started.get());
          break;
        case ERROR:
          log.error(message, started.get());
          break;
      }
    }
  }

  @Override
  public void emit(Event event)
  {
    synchronized (started) {
      if (!started.get()) {
        throw new RejectedExecutionException("Service not started.");
      }
    }
    try {
      switch (level) {
        case TRACE:
          if (log.isTraceEnabled()) {
            log.trace(jsonMapper.writeValueAsString(event));
          }
          break;
        case DEBUG:
          if (log.isDebugEnabled()) {
            log.debug(jsonMapper.writeValueAsString(event));
          }
          break;
        case INFO:
          if (log.isInfoEnabled()) {
            log.info(jsonMapper.writeValueAsString(event));
          }
          break;
        case WARN:
          log.warn(jsonMapper.writeValueAsString(event));
          break;
        case ERROR:
          log.error(jsonMapper.writeValueAsString(event));
          break;
      }
    }
    catch (Exception e) {
      log.warn(e, "Failed to generate json");
    }
  }

  @Override
  public void flush()
  {

  }

  @Override
  @LifecycleStop
  public void close()
  {
    final boolean wasStarted = started.getAndSet(false);
    if (wasStarted) {
      final String message = "Close: started [%s]";
      switch (level) {
        case TRACE:
          if (log.isTraceEnabled()) {
            log.trace(message, started.get());
          }
          break;
        case DEBUG:
          if (log.isDebugEnabled()) {
            log.debug(message, started.get());
          }
          break;
        case INFO:
          if (log.isInfoEnabled()) {
            log.info(message, started.get());
          }
          break;
        case WARN:
          log.warn(message, started.get());
          break;
        case ERROR:
          log.error(message, started.get());
          break;
      }
    }
  }

  @Override
  public String toString()
  {
    return "LoggingEmitter{" +
           "log=" + log +
           ", level=" + level +
           '}';
  }

  public enum Level
  {
    TRACE,
    DEBUG,
    INFO,
    WARN,
    ERROR;

    public static Level toLevel(String name)
    {
      return Level.valueOf(StringUtils.toUpperCase(name));
    }
  }
}
