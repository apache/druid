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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.Query;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.server.RequestLogLine;
import org.slf4j.MDC;

import java.io.IOException;
import java.util.Map;

public class LoggingRequestLogger implements RequestLogger
{
  private static final Logger LOG = new Logger(LoggingRequestLogger.class);

  private final ObjectMapper mapper;
  private final boolean setMDC;
  private final boolean setContextMDC;

  public LoggingRequestLogger(
      ObjectMapper mapper,
      boolean setMDC,
      boolean setContextMDC
  )
  {
    this.mapper = mapper;
    this.setMDC = setMDC;
    this.setContextMDC = setContextMDC;
  }

  @Override
  public void logNativeQuery(RequestLogLine requestLogLine) throws IOException
  {
    final Map mdc = MDC.getCopyOfContextMap();
    // MDC must be set during the `LOG.info` call at the end of the try block.
    try {
      if (setMDC) {
        try {
          final Query query = requestLogLine.getQuery();
          MDC.put("queryId", query.getId());
          MDC.put("sqlQueryId", StringUtils.nullToEmptyNonDruidDataString(query.getSqlQueryId()));
          MDC.put("dataSource", String.join(",", query.getDataSource().getTableNames()));
          MDC.put("queryType", query.getType());
          MDC.put("isNested", String.valueOf(!(query.getDataSource() instanceof TableDataSource)));
          MDC.put("hasFilters", Boolean.toString(query.hasFilters()));
          MDC.put("remoteAddr", requestLogLine.getRemoteAddr());
          MDC.put("duration", query.getDuration().toString());
          MDC.put("descending", Boolean.toString(query.isDescending()));
          if (setContextMDC) {
            final Iterable<Map.Entry<String, Object>> entries = query.getContext() == null
                                                                ? ImmutableList.of()
                                                                : query.getContext().entrySet();
            for (Map.Entry<String, Object> entry : entries) {
              MDC.put(entry.getKey(), entry.getValue() == null ? "NULL" : entry.getValue().toString());
            }
          }
        }
        catch (RuntimeException re) {
          LOG.error(re, "Error preparing MDC");
        }
      }
      final String line = requestLogLine.getNativeQueryLine(mapper);

      // MDC must be set here
      LOG.info("%s", line);
    }
    finally {
      if (setMDC) {
        if (mdc != null) {
          MDC.setContextMap(mdc);
        } else {
          MDC.clear();
        }
      }
    }
  }

  @Override
  public void logSqlQuery(RequestLogLine requestLogLine) throws IOException
  {
    final String line = requestLogLine.getSqlQueryLine(mapper);
    LOG.info("%s", line);
  }

  public boolean isSetMDC()
  {
    return setMDC;
  }

  public boolean isSetContextMDC()
  {
    return setContextMDC;
  }

  @Override
  public String toString()
  {
    return "LoggingRequestLogger{" +
           "setMDC=" + setMDC +
           ", setContextMDC=" + setContextMDC +
           '}';
  }
}
