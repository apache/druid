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
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.RequestLogLine;

import javax.validation.constraints.NotNull;
import java.io.IOException;

@JsonTypeName("switching")
public class SwitchingRequestLoggerProvider implements RequestLoggerProvider
{
  private static final Logger log = new Logger(SwitchingRequestLoggerProvider.class);

  @JsonProperty
  @NotNull
  private RequestLoggerProvider nativeQueryLogger;

  @JsonProperty
  @NotNull
  private RequestLoggerProvider sqlQueryLogger;

  @Override
  public RequestLogger get()
  {
    SwitchingRequestLogger logger = new SwitchingRequestLogger(nativeQueryLogger.get(), sqlQueryLogger.get());
    log.debug(new Exception("Stack trace"), "Creating %s at", logger);
    return logger;
  }

  public static class SwitchingRequestLogger implements RequestLogger
  {
    private final RequestLogger nativeQueryLogger;
    private final RequestLogger sqlQueryLogger;

    public SwitchingRequestLogger(RequestLogger nativeQueryLogger, RequestLogger sqlQueryLogger)
    {
      this.nativeQueryLogger = nativeQueryLogger;
      this.sqlQueryLogger = sqlQueryLogger;
    }

    @Override
    public void logNativeQuery(RequestLogLine requestLogLine) throws IOException
    {
      nativeQueryLogger.logNativeQuery(requestLogLine);
    }

    @Override
    public void logSqlQuery(RequestLogLine requestLogLine) throws IOException
    {
      sqlQueryLogger.logSqlQuery(requestLogLine);
    }

    @LifecycleStart
    @Override
    public void start() throws Exception
    {
      nativeQueryLogger.start();
      sqlQueryLogger.start();
    }

    @LifecycleStop
    @Override
    public void stop()
    {
      nativeQueryLogger.stop();
      sqlQueryLogger.stop();
    }

    @Override
    public String toString()
    {
      return "SwitchingRequestLogger{" +
             "nativeQueryLogger=" + nativeQueryLogger +
             ", sqlQueryLogger=" + sqlQueryLogger +
             '}';
    }
  }
}
