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

package org.apache.druid.server.log;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Throwables;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.RequestLogLine;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 */
@JsonTypeName("composing")
public class ComposingRequestLoggerProvider implements RequestLoggerProvider
{
  private static final Logger log = new Logger(ComposingRequestLoggerProvider.class);

  @JsonProperty
  @NotNull
  private final List<RequestLoggerProvider> loggerProviders = new ArrayList<>();

  @Override
  public RequestLogger get()
  {
    final List<RequestLogger> loggers = new ArrayList<>();
    for (RequestLoggerProvider loggerProvider : loggerProviders) {
      loggers.add(loggerProvider.get());
    }
    ComposingRequestLogger logger = new ComposingRequestLogger(loggers);
    log.debug(new Exception("Stack trace"), "Creating %s at", logger);
    return logger;
  }

  public static class ComposingRequestLogger implements RequestLogger
  {
    private final List<RequestLogger> loggers;

    public ComposingRequestLogger(List<RequestLogger> loggers)
    {
      this.loggers = loggers;
    }

    @LifecycleStart
    @Override
    public void start() throws Exception
    {
      for (RequestLogger logger : loggers) {
        logger.start();
      }
    }

    @LifecycleStop
    @Override
    public void stop()
    {
      for (RequestLogger logger : loggers) {
        logger.stop();
      }
    }

    @Override
    public void logNativeQuery(RequestLogLine requestLogLine) throws IOException
    {
      delegateToAllChildren(requestLogLine, RequestLogger::logNativeQuery);
    }

    @Override
    public void logSqlQuery(RequestLogLine requestLogLine) throws IOException
    {
      delegateToAllChildren(requestLogLine, RequestLogger::logSqlQuery);
    }

    private void delegateToAllChildren(RequestLogLine requestLogLine, RequestLogLineConsumer consumer) throws IOException
    {
      Exception exception = null;
      for (RequestLogger logger : loggers) {
        try {
          consumer.accept(logger, requestLogLine);
        }
        catch (Exception e) {
          if (exception == null) {
            exception = e;
          } else {
            exception.addSuppressed(e);
          }
        }
      }
      if (exception != null) {
        Throwables.propagateIfInstanceOf(exception, IOException.class);
        throw new RuntimeException(exception);
      }
    }

    @Override
    public String toString()
    {
      return "ComposingRequestLogger{" +
             "loggers=" + loggers +
             '}';
    }

    private interface RequestLogLineConsumer
    {
      @SuppressWarnings("RedundantThrows")
      void accept(RequestLogger requestLogger, RequestLogLine requestLogLine) throws IOException;
    }
  }

}
