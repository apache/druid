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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Filter;
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
 * This class enforces console logging based on a user defined configuration.
 * <p>
 * For all loggers, this class ensure that only {@link ConsoleAppender} is applied for each of them.
 * <p>
 * The reason why this configuration is still based on a user's configuration is that
 * user can still configure different logging levels for different loggers in that file.
 */
public class ConsoleLoggingEnforcementConfigurationFactory extends ConfigurationFactory
{
  /**
   * Valid file extensions for XML files.
   */
  public static final String[] SUFFIXES = new String[]{".xml", "*"};

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

  /**
   * override the original configuration source to ensure only console appender is applied
   */
  static class OverrideConfiguration extends XmlConfiguration
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
        applyConsoleAppender(logger, consoleAppender);
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

    /**
     * remove all appenders from a logger and append a console appender to it
     */
    private void applyConsoleAppender(LoggerConfig logger, Appender consoleAppender)
    {
      if (logger.getAppenderRefs().size() == 1
          && logger.getAppenderRefs().get(0).getRef().equals(consoleAppender.getName())) {
        // this logger has only one appender and its the console appender
        return;
      }

      Level level = Level.INFO;
      Filter filter = null;

      if (!logger.getAppenderRefs().isEmpty()) {
        AppenderRef appenderRef = logger.getAppenderRefs().get(0);

        // clear all appenders first
        List<String> appendRefs = logger.getAppenderRefs()
                                        .stream()
                                        .map(AppenderRef::getRef)
                                        .collect(Collectors.toList());
        appendRefs.forEach(logger::removeAppender);

        // use the first appender's definition
        level = appenderRef.getLevel();
        filter = appenderRef.getFilter();
      }

      // add ConsoleAppender to this logger
      logger.addAppender(consoleAppender, level, filter);
    }
  }
}
