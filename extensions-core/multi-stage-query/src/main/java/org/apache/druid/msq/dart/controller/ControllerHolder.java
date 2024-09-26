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

package org.apache.druid.msq.dart.controller;

import com.google.common.base.Preconditions;
import org.apache.druid.msq.exec.Controller;
import org.apache.druid.server.security.AuthenticationResult;
import org.joda.time.DateTime;

/**
 * Holder for {@link Controller}, stored in {@link DartControllerRegistry}.
 */
public class ControllerHolder
{
  private final Controller controller;
  private final String sqlQueryId;
  private final String sql;
  private final AuthenticationResult authenticationResult;
  private final DateTime startTime;

  public ControllerHolder(
      final Controller controller,
      final String sqlQueryId,
      final String sql,
      final AuthenticationResult authenticationResult,
      final DateTime startTime
  )
  {
    this.controller = Preconditions.checkNotNull(controller, "controller");
    this.sqlQueryId = Preconditions.checkNotNull(sqlQueryId, "sqlQueryId");
    this.sql = sql;
    this.authenticationResult = authenticationResult;
    this.startTime = Preconditions.checkNotNull(startTime, "startTime");
  }

  public Controller getController()
  {
    return controller;
  }

  public String getSqlQueryId()
  {
    return sqlQueryId;
  }

  public String getSql()
  {
    return sql;
  }

  public AuthenticationResult getAuthenticationResult()
  {
    return authenticationResult;
  }

  public DateTime getStartTime()
  {
    return startTime;
  }
}
