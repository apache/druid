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
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.RequestLogLine;

import javax.validation.constraints.NotNull;
import java.io.IOException;

/**
 */
@JsonTypeName("filtered")
public class FilteredRequestLoggerProvider implements RequestLoggerProvider
{
  private static final Logger log = new Logger(FilteredRequestLoggerProvider.class);

  @JsonProperty
  @NotNull
  private RequestLoggerProvider delegate = null;

  @JsonProperty
  private long queryTimeThresholdMs = 0;

  @Override
  public RequestLogger get()
  {
    FilteredRequestLogger logger = new FilteredRequestLogger(delegate.get(), queryTimeThresholdMs);
    log.debug(new Exception("Stack trace"), "Creating %s at", logger);
    return logger;
  }

  public static class FilteredRequestLogger implements RequestLogger
  {

    private final long queryTimeThresholdMs;
    private final RequestLogger logger;

    public FilteredRequestLogger(RequestLogger logger, long queryTimeThresholdMs)
    {
      this.logger = logger;
      this.queryTimeThresholdMs = queryTimeThresholdMs;
    }

    @Override
    public void log(RequestLogLine requestLogLine) throws IOException
    {
      Object queryTime = requestLogLine.getQueryStats().getStats().get("query/time");
      if (queryTime != null && ((Number) queryTime).longValue() >= queryTimeThresholdMs) {
        logger.log(requestLogLine);
      }
    }

    @Override
    public String toString()
    {
      return "FilteredRequestLogger{" +
             "queryTimeThresholdMs=" + queryTimeThresholdMs +
             ", logger=" + logger +
             '}';
    }
  }

}
