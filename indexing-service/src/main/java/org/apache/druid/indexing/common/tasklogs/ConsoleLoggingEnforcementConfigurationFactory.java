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

package org.apache.druid.indexing.common.tasklogs;

import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.AppenderRef;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.xml.XmlConfiguration;
import org.apache.logging.log4j.core.layout.PatternLayout;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class enforces console logging on a user defined configuration.
 * <p>
 * If log appender in user defined configuration is not configured to {@link ConsoleAppender},
 * this classes will replace appenders of all loggers to this {@link ConsoleAppender}.
 * <p>
 * The reason why the configuration is still based on user's configuration is that
 * user can still configure different logging levels for different loggers in the configuration file.
 */
public class ConsoleLoggingEnforcementConfigurationFactory extends ConfigurationFactory
{
  /**
   * Valid file extensions for XML files.
   */
  public static final String[] SUFFIXES = new String[]{".xml", "*"};

  private static void applyConsoleAppender(LoggerConfig logger, Appender consoleAppender)
  {
    // TODO: how about appenders is empty?
    AppenderRef appenderRef = logger.getAppenderRefs().get(0);

    // clear all appenders first
    List<String> appendRefs = logger.getAppenderRefs().stream().map(AppenderRef::getRef).collect(Collectors.toList());
    appendRefs.forEach(logger::removeAppender);

    // add ConsoleAppender to this logger
    logger.addAppender(consoleAppender, appenderRef.getLevel(), appenderRef.getFilter());
  }

  @Override
  public String[] getSupportedTypes()
  {
    return SUFFIXES;
  }

  @Override
  public Configuration getConfiguration(LoggerContext loggerContext, ConfigurationSource source)
  {
    return new OverrideConfiguration(loggerContext, source);
  }

  public static class OverrideConfiguration extends XmlConfiguration
  {
    public OverrideConfiguration(final LoggerContext loggerContext, final ConfigurationSource configSource)
    {
      super(loggerContext, configSource);
    }

    @Override
    protected void doConfigure()
    {
      super.doConfigure();

      Appender consoleAppender = findConsoleAppender();
      if (consoleAppender == null) {
        // create a ConsoleAppender with default pattern if no console appender is configured in the configuration file
        consoleAppender = ConsoleAppender.newBuilder()
                                         .setName("_Injected_Console_Appender_")
                                         .setLayout(PatternLayout.newBuilder()
                                                                 .withPattern("%d{ISO8601} %p [%t] %c - %m%n")
                                                                 .build())
                                         .build();
      }

      List<LoggerConfig> loggerConfigList = new ArrayList<>();
      loggerConfigList.add(this.getRootLogger());
      loggerConfigList.addAll(this.getLoggers().values());

      //
      // For all logger configuration, check if its appender is ConsoleAppender.
      // If not, replace it's appender to ConsoleAppender.
      //
      for (LoggerConfig logger : loggerConfigList) {
        if (!hasConsoleAppender(logger, consoleAppender.getName())) {
          applyConsoleAppender(logger, consoleAppender);
        }
      }
    }

    @Nullable
    private Appender findConsoleAppender()
    {
      for (Map.Entry<String, Appender> entry : this.getAppenders().entrySet()) {
        Appender appender = entry.getValue();
        if (appender instanceof ConsoleAppender) {
          return appender;
        }
      }
      return null;
    }

    private boolean hasConsoleAppender(LoggerConfig logger, String consoleAppenderName)
    {
      for (AppenderRef ref : logger.getAppenderRefs()) {
        if (consoleAppenderName.equals(ref.getRef())) {
          return true;
        }
      }
      return false;
    }
  }
}
