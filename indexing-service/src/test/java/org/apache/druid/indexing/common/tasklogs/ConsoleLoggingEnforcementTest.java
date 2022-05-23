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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ConsoleLoggingEnforcementTest
{
  private static final String ROOT = "ROOT";

  @Test
  public void testConsoleConfiguration() throws IOException
  {
    // the loggers in configuration already uses Console appender
    String log4jConfiguration = "<Configuration status=\"WARN\">\n"
                                + "  <Appenders>\n"
                                + "    <Console name=\"Console\" target=\"SYSTEM_OUT\">\n"
                                + "      <PatternLayout pattern=\"%m\"/>\n"
                                + "    </Console>\n"
                                + "  </Appenders>\n"
                                + "  <Loggers>\n"
                                + "    <Root level=\"info\">\n"
                                + "      <AppenderRef ref=\"Console\"/>\n"
                                + "    </Root>\n"
                                + "    <Logger level=\"debug\" name=\"org.apache.druid\" additivity=\"false\">\n"
                                + "      <AppenderRef ref=\"Console\"/>\n"
                                + "    </Logger>\n"
                                + "  </Loggers>\n"
                                + "</Configuration>";

    LoggerContext context = enforceConsoleLogger(log4jConfiguration);

    // this logger is not defined in configuration, it derivates ROOT logger configuration
    assertHasOnlyOneConsoleAppender(getLogger(context, "name_not_in_config"), Level.INFO);
    assertHasOnlyOneConsoleAppender(getLogger(context, "org.apache.druid"), Level.DEBUG);
    assertHasOnlyOneConsoleAppender(getLogger(context, ROOT), Level.INFO);

    PatternLayout layout = (PatternLayout) getLogger(context, "anything").getAppenders()
                                                                         .values()
                                                                         .stream()
                                                                         .findFirst()
                                                                         .get()
                                                                         .getLayout();
    Assert.assertEquals("%m", layout.getConversionPattern());
  }

  @Test
  public void testNoConsoleAppender() throws IOException
  {
    // this logger configuration has no console logger appender, a default one will be created
    String log4jConfiguration = "<Configuration status=\"WARN\">\n"
                                + "  <Appenders>\n"
                                + "    <RollingRandomAccessFile name=\"FileAppender\" fileName=\"a.log\">\n"
                                + "      <PatternLayout pattern=\"%d{ISO8601} %p [%t] %c - %m%n\"/>\n"
                                + "    </RollingRandomAccessFile>\n"
                                + "  </Appenders>\n"
                                + "  <Loggers>\n"
                                + "    <Root level=\"info\">\n"
                                + "      <AppenderRef ref=\"FileAppender\"/>\n"
                                + "    </Root>\n"
                                + "  </Loggers>\n"
                                + "</Configuration>";

    LoggerContext context = enforceConsoleLogger(log4jConfiguration);

    // this logger is not defined in configuration, it derivates ROOT logger configuration
    assertHasOnlyOneConsoleAppender(getLogger(context, "name_not_in_config"), Level.INFO);
    assertHasOnlyOneConsoleAppender(getLogger(context, ROOT), Level.INFO);

    String defaultPattern = "%d{ISO8601} %p [%t] %c - %m%n";
    PatternLayout layout = (PatternLayout) getLogger(context, "anything").getAppenders()
                                                                         .values()
                                                                         .stream()
                                                                         .findFirst()
                                                                         .get()
                                                                         .getLayout();
    Assert.assertEquals(defaultPattern, layout.getConversionPattern());
  }

  @Test
  public void testHasConsoleAppenderButNotUsed() throws IOException
  {
    // this logger has a console appender, but is not referenced by any logger
    String log4jConfiguration = "<Configuration status=\"WARN\">\n"
                                + "  <Appenders>\n"
                                + "    <Console name=\"Console\" target=\"SYSTEM_OUT\">\n"
                                + "      <PatternLayout pattern=\"%m\"/>\n"
                                + "    </Console>\n"
                                + "    <RollingRandomAccessFile name=\"FileAppender\" fileName=\"a.log\">\n"
                                + "      <PatternLayout pattern=\"%d{ISO8601} %p [%t] %c - %m%n\"/>\n"
                                + "    </RollingRandomAccessFile>\n"
                                + "  </Appenders>\n"
                                + "  <Loggers>\n"
                                + "    <Root level=\"info\">\n"
                                + "      <AppenderRef ref=\"FileAppender\"/>\n"
                                + "    </Root>\n"
                                + "  </Loggers>\n"
                                + "</Configuration>";

    LoggerContext context = enforceConsoleLogger(log4jConfiguration);

    // this logger is not defined in configuration, it derivates ROOT logger configuration
    assertHasOnlyOneConsoleAppender(getLogger(context, "name_not_in_config"), Level.INFO);

    assertHasOnlyOneConsoleAppender(getLogger(context, ROOT), Level.INFO);

    // the ConsoleAppender should be exactly the same as it's in the configuration
    PatternLayout layout = (PatternLayout) getLogger(context, "anything").getAppenders()
                                                                         .values()
                                                                         .stream()
                                                                         .findFirst()
                                                                         .get()
                                                                         .getLayout();
    Assert.assertEquals("%m", layout.getConversionPattern());
  }

  @Test
  public void testMultipleAppender() throws IOException
  {
    // this logger configuration contains multiple appenders and appender refers
    String log4jConfiguration = "<Configuration status=\"WARN\">\n"
                                + "  <Appenders>\n"
                                + "    <Console name=\"Console\" target=\"SYSTEM_OUT\">\n"
                                + "      <PatternLayout pattern=\"%m\"/>\n"
                                + "    </Console>\n"
                                + "    <RollingRandomAccessFile name=\"FileAppender\" fileName=\"a.log\">\n"
                                + "      <PatternLayout pattern=\"%d{ISO8601} %p [%t] %c - %m%n\"/>\n"
                                + "    </RollingRandomAccessFile>\n"
                                + "  </Appenders>\n"
                                + "  <Loggers>\n"
                                + "    <Root level=\"info\">\n"
                                + "      <AppenderRef ref=\"FileAppender\"/>\n"
                                + "      <AppenderRef ref=\"Console\"/>\n"
                                + "    </Root>\n"
                                + "    <Logger level=\"debug\" name=\"org.apache.druid\" additivity=\"false\">\n"
                                + "      <AppenderRef ref=\"FileAppender\"/>\n"
                                + "      <AppenderRef ref=\"Console\"/>\n"
                                + "    </Logger>\n"
                                + "  </Loggers>\n"
                                + "</Configuration>";

    LoggerContext context = enforceConsoleLogger(log4jConfiguration);

    // this logger is not defined in configuration, it derivates ROOT logger configuration
    assertHasOnlyOneConsoleAppender(getLogger(context, "name_not_in_config"), Level.INFO);

    assertHasOnlyOneConsoleAppender(getLogger(context, "org.apache.druid"), Level.DEBUG);
    assertHasOnlyOneConsoleAppender(getLogger(context, ROOT), Level.INFO);

    // the ConsoleAppender should be exactly the same as it's in the configuration
    PatternLayout layout = (PatternLayout) getLogger(context, "anything").getAppenders()
                                                                         .values()
                                                                         .stream()
                                                                         .findFirst()
                                                                         .get()
                                                                         .getLayout();
    Assert.assertEquals("%m", layout.getConversionPattern());
  }

  @Test
  public void testEmptyAppender() throws IOException
  {
    // the ROOT logger has no appender in this configuration
    String log4jConfiguration = "<Configuration status=\"WARN\">\n"
                                + "  <Appenders>\n"
                                + "    <Console name=\"Console\" target=\"SYSTEM_OUT\">\n"
                                + "      <PatternLayout pattern=\"%m\"/>\n"
                                + "    </Console>\n"
                                + "    <RollingRandomAccessFile name=\"FileAppender\" fileName=\"a.log\">\n"
                                + "      <PatternLayout pattern=\"%d{ISO8601} %p [%t] %c - %m%n\"/>\n"
                                + "    </RollingRandomAccessFile>\n"
                                + "  </Appenders>\n"
                                + "  <Loggers>\n"
                                + "    <Root level=\"info\">\n"
                                + "    </Root>\n"
                                + "    <Logger level=\"debug\" name=\"org.apache.druid\" additivity=\"false\">\n"
                                + "      <AppenderRef ref=\"FileAppender\"/>\n"
                                + "      <AppenderRef ref=\"Console\"/>\n"
                                + "    </Logger>\n"
                                + "  </Loggers>\n"
                                + "</Configuration>";

    LoggerContext context = enforceConsoleLogger(log4jConfiguration);

    // this logger is not defined in configuration, it derivates ROOT logger configuration
    assertHasOnlyOneConsoleAppender(getLogger(context, "name_not_in_config"), Level.INFO);

    assertHasOnlyOneConsoleAppender(getLogger(context, "org.apache.druid"), Level.DEBUG);
    assertHasOnlyOneConsoleAppender(getLogger(context, ROOT), Level.INFO);

    // the ConsoleAppender should be exactly the same as it's in the configuration
    PatternLayout layout = (PatternLayout) getLogger(context, "anything").getAppenders()
                                                                         .values()
                                                                         .stream()
                                                                         .findFirst()
                                                                         .get()
                                                                         .getLayout();
    Assert.assertEquals("%m", layout.getConversionPattern());
  }

  private LoggerContext enforceConsoleLogger(String configuration) throws IOException
  {
    LoggerContext context = new LoggerContext("test");
    ConfigurationSource source = new ConfigurationSource(new ByteArrayInputStream(configuration.getBytes(StandardCharsets.UTF_8)));

    // enforce the console logging for current configuration
    context.reconfigure(new ConsoleLoggingEnforcementConfigurationFactory().getConfiguration(context, source));
    return context;
  }

  private void assertHasOnlyOneConsoleAppender(Logger logger, Level level)
  {
    // there's only one appender
    Assert.assertEquals(1, logger.getAppenders().size());

    // and this appender must be ConsoleAppender
    Assert.assertEquals(ConsoleAppender.class, logger.getAppenders()
                                                     .values()
                                                     .stream()
                                                     .findFirst()
                                                     .get()
                                                     .getClass());
    if (level != null) {
      Assert.assertEquals(level, logger.getLevel());
    }
  }

  private Logger getLogger(LoggerContext context, String name)
  {
    final String key = ROOT.equals(name) ? LogManager.ROOT_LOGGER_NAME : name;
    return context.getLogger(key);
  }
}
