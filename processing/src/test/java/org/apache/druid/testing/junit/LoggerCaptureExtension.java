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

package org.apache.druid.testing.junit;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.ArrayList;
import java.util.List;

/**
 * JUnit 5 extension that captures Log4j events emitted by a target class.
 * Register it with {@code new LoggerCaptureExtension(TargetClass.class)} and inspect {@link #getLogEvents()}.
 */
public class LoggerCaptureExtension implements BeforeEachCallback, AfterEachCallback
{
  private final Class<?> targetClass;
  private InMemoryAppender inMemoryAppender;
  private LoggerConfig targetClassLoggerConfig;

  public LoggerCaptureExtension(final Class<?> targetClass)
  {
    this.targetClass = targetClass;
  }

  @Override
  public void beforeEach(final ExtensionContext context)
  {
    inMemoryAppender = new InMemoryAppender(targetClass);
    final LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);
    final Configuration configuration = loggerContext.getConfiguration();
    targetClassLoggerConfig = configuration.getLoggerConfig(targetClass.getName());
    targetClassLoggerConfig.addAppender(inMemoryAppender, Level.ALL, null);
  }

  @Override
  public void afterEach(final ExtensionContext context)
  {
    clearLogEvents();
    targetClassLoggerConfig.removeAppender(InMemoryAppender.NAME);
  }

  public List<LogEvent> getLogEvents()
  {
    return inMemoryAppender.getLogEvents();
  }

  public void clearLogEvents()
  {
    inMemoryAppender.clearLogEvents();
  }

  public void awaitLogEvents() throws InterruptedException
  {
    inMemoryAppender.awaitLogEvents();
  }

  private static class InMemoryAppender extends AbstractAppender
  {
    private static final String NAME = InMemoryAppender.class.getName();
    private final String targetLoggerName;
    @GuardedBy("logEvents")
    private final List<LogEvent> logEvents = new ArrayList<>();

    InMemoryAppender(final Class<?> targetClass)
    {
      super(NAME, null, null, true, Property.EMPTY_ARRAY);
      targetLoggerName = targetClass.getName();
    }

    @Override
    public void append(final LogEvent logEvent)
    {
      synchronized (logEvents) {
        if (logEvent.getLoggerName().equals(targetLoggerName)) {
          logEvents.add(logEvent);
          logEvents.notifyAll();
        }
      }
    }

    List<LogEvent> getLogEvents()
    {
      synchronized (logEvents) {
        return ImmutableList.copyOf(logEvents);
      }
    }

    void clearLogEvents()
    {
      synchronized (logEvents) {
        logEvents.clear();
      }
    }

    void awaitLogEvents() throws InterruptedException
    {
      synchronized (logEvents) {
        while (logEvents.isEmpty()) {
          logEvents.wait();
        }
      }
    }
  }
}
