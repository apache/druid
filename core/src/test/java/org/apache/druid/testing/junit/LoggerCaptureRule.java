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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.rules.ExternalResource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * JUnit rule to capture a class's logger output to an in-memory buffer to allow verification of log messages in tests.
 */
public class LoggerCaptureRule extends ExternalResource
{
  private final Class<?> targetClass;

  private InMemoryAppender inMemoryAppender;
  private LoggerConfig targetClassLoggerConfig;

  public LoggerCaptureRule(Class<?> targetClass)
  {
    this.targetClass = targetClass;
  }

  @Override
  protected void before()
  {
    inMemoryAppender = new InMemoryAppender();
    LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);
    Configuration configuration = loggerContext.getConfiguration();
    targetClassLoggerConfig = configuration.getLoggerConfig(targetClass.getName());
    targetClassLoggerConfig.addAppender(inMemoryAppender, Level.ALL, null);
  }

  @Override
  protected void after()
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

  private static class InMemoryAppender extends AbstractAppender
  {
    static final String NAME = InMemoryAppender.class.getName();

    private final List<LogEvent> logEvents;

    InMemoryAppender()
    {
      super(NAME, null, null);
      logEvents = new ArrayList<>();
    }

    @Override
    public void append(LogEvent logEvent)
    {
      logEvents.add(logEvent);
    }

    List<LogEvent> getLogEvents()
    {
      return Collections.unmodifiableList(logEvents);
    }

    void clearLogEvents()
    {
      logEvents.clear();
    }
  }
}

