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

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.AppenderRef;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.xml.XmlConfiguration;
import org.apache.logging.log4j.core.layout.PatternLayout;

import javax.annotation.Nonnull;
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

  private static final Logger log = new Logger(ConsoleLoggingEnforcementConfigurationFactory.class);

  /**
   * Valid file extensions for XML files.
   */
  public static final String[] SUFFIXES = new String[]{".xml", "*"};

  // Alter log level for this class to be warning. This needs to happen because the logger is using the default
  // config, which is always level error and appends to console, since the logger is being configured by this class.
  static {
    Configurator.setLevel(log.getName(), Level.WARN);
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

      List<Appender> consoleAppenders = findConsoleAppenders();
      if (consoleAppenders.isEmpty()) {
        // create a ConsoleAppender with default pattern if no console appender is configured in the configuration file
        Appender injectedConsoleAppender = ConsoleAppender.newBuilder()
                                                          .setName("_Injected_Console_Appender_")
                                                          .setLayout(
                                                              PatternLayout.newBuilder()
                                                                           .withPattern("%d{ISO8601} %p [%t] %c - %m%n")
                                                                           .build()
                                                          )
                                                          .build();
        injectedConsoleAppender.start();
        consoleAppenders.add(injectedConsoleAppender);
      }

      List<LoggerConfig> loggerConfigList = new ArrayList<>();
      loggerConfigList.add(this.getRootLogger());
      loggerConfigList.addAll(this.getLoggers().values());


      //
      // For all logger configuration, check if its appender is ConsoleAppender.
      // If not, replace it's appender to ConsoleAppender.
      //
      for (LoggerConfig logger : loggerConfigList) {
        applyConsoleAppender(logger, consoleAppenders);
      }
    }

    @Nonnull
    private List<Appender> findConsoleAppenders()
    {
      List<Appender> consoleAppenders = new ArrayList<>();
      for (Map.Entry<String, Appender> entry : this.getAppenders().entrySet()) {
        Appender appender = entry.getValue();
        if (appender instanceof ConsoleAppender) {
          consoleAppenders.add(appender);
        }
      }
      return consoleAppenders;
    }

    /**
     * Ensure there is a console logger defined. Without a console logger peon logs wont be able to be stored in deep storage
     */
    private void applyConsoleAppender(LoggerConfig logger, List<Appender> consoleAppenders)
    {
      List<String> consoleAppenderNames = consoleAppenders.stream().map(Appender::getName).collect(Collectors.toList());
      for (AppenderRef appenderRef : logger.getAppenderRefs()) {
        if (consoleAppenderNames.contains(appenderRef.getRef())) {
          // we need a console logger no matter what, but we want to be able to define a different appender if necessary
          return;
        }
      }
      Level level = Level.INFO;
      Filter filter = null;
      Appender consoleAppender = consoleAppenders.get(0);
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
        log.warn("Clearing all configured appenders for logger %s. Using %s instead.",
                 logger.toString(),
                 consoleAppender.getName());
      }

      // add ConsoleAppender to this logger
      logger.addAppender(consoleAppender, level, filter);
    }
  }
}
