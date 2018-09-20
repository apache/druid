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

package org.apache.druid.sql.log;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.java.util.common.logger.Logger;

import javax.validation.constraints.NotNull;
import java.io.IOException;

@JsonTypeName("filtered")
public class FilteredSqlRequestLoggerProvider implements SqlRequestLoggerProvider
{
  private static final Logger log = new Logger(FilteredSqlRequestLoggerProvider.class);

  @JsonProperty
  @NotNull
  private SqlRequestLoggerProvider delegate = null;

  @JsonProperty
  private long sqlTimeThresholdMs = 0;

  @Override
  public SqlRequestLogger get()
  {
    FilteredSqlRequestLogger logger = new FilteredSqlRequestLogger(delegate.get(), sqlTimeThresholdMs);
    log.debug(new Exception("Stack trace"), "Creating %s at", logger);
    return logger;
  }

  public static class FilteredSqlRequestLogger implements SqlRequestLogger
  {
    private final SqlRequestLogger delegate;
    private final long sqlTimeThresholdMs;

    public FilteredSqlRequestLogger(SqlRequestLogger delegate, long sqlTimeThresholdMs)
    {
      this.delegate = delegate;
      this.sqlTimeThresholdMs = sqlTimeThresholdMs;
    }

    @Override
    public void log(SqlRequestLogLine sqlRequestLogLine) throws IOException
    {
      Object sqlTime = sqlRequestLogLine.getQueryStats().getStats().get("sqlQuery/time");
      if (sqlTime != null && ((Number) sqlTime).longValue() >= sqlTimeThresholdMs) {
        delegate.log(sqlRequestLogLine);
      }
    }

    @Override
    public void start() throws IOException
    {
      delegate.start();
    }

    @Override
    public void stop()
    {
      delegate.stop();
    }

    @Override
    public String toString()
    {
      return "FilteredSqlRequestLogger{" +
             "delegate=" + delegate +
             ", sqlTimeThresholdMs=" + sqlTimeThresholdMs +
             '}';
    }
  }
}
