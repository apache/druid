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
import org.apache.druid.server.QueryStats;
import org.apache.druid.server.RequestLogLineBase;
import org.joda.time.DateTime;

import java.util.Objects;

public class SqlRequestLogLine extends RequestLogLineBase
{
  private final String sql;

  public SqlRequestLogLine(DateTime timestamp, String remoteAddr, String sql, QueryStats queryStats)
  {
    super(timestamp, remoteAddr, queryStats);
    this.sql = sql;
  }

  @JsonProperty("sql")
  public String getSql()
  {
    return sql;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SqlRequestLogLine)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    SqlRequestLogLine that = (SqlRequestLogLine) o;
    return Objects.equals(sql, that.sql);
  }

  @Override
  public int hashCode()
  {

    return Objects.hash(super.hashCode(), sql);
  }

  @Override
  public String toString()
  {
    return "SqlRequestLogLine{" +
           "sql='" + sql + '\'' +
           ", timestamp=" + timestamp +
           ", remoteAddr='" + remoteAddr + '\'' +
           ", queryStats=" + queryStats +
           '}';
  }
}

