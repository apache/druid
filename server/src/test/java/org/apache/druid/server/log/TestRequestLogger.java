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

import com.google.common.collect.ImmutableList;
import org.apache.druid.server.RequestLogLine;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestRequestLogger implements RequestLogger
{
  private final List<RequestLogLine> nativeQuerylogs;
  private final List<RequestLogLine> sqlQueryLogs;
  private final AtomicBoolean started = new AtomicBoolean();

  public TestRequestLogger()
  {
    this.nativeQuerylogs = new ArrayList<>();
    this.sqlQueryLogs = new ArrayList<>();
  }

  @Override
  public void start()
  {
    started.set(true);
  }

  @Override
  public void stop()
  {
    started.set(false);
  }

  public boolean isStarted()
  {
    return started.get();
  }

  @Override
  public void logNativeQuery(final RequestLogLine requestLogLine)
  {
    synchronized (nativeQuerylogs) {
      nativeQuerylogs.add(requestLogLine);
    }
  }

  @Override
  public void logSqlQuery(RequestLogLine requestLogLine)
  {
    synchronized (sqlQueryLogs) {
      sqlQueryLogs.add(requestLogLine);
    }
  }

  public List<RequestLogLine> getNativeQuerylogs()
  {
    synchronized (nativeQuerylogs) {
      return ImmutableList.copyOf(nativeQuerylogs);
    }
  }

  public List<RequestLogLine> getSqlQueryLogs()
  {
    synchronized (sqlQueryLogs) {
      return ImmutableList.copyOf(sqlQueryLogs);
    }
  }

  public void clear()
  {
    synchronized (nativeQuerylogs) {
      nativeQuerylogs.clear();
    }
    synchronized (sqlQueryLogs) {
      sqlQueryLogs.clear();
    }
  }
}
