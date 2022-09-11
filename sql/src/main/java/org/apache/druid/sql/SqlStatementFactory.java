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

package org.apache.druid.sql;

import org.apache.druid.sql.http.SqlQuery;

import javax.servlet.http.HttpServletRequest;

/**
 * A class for the creation of Statements, which happen to be used for Sql.
 */
public class SqlStatementFactory
{
  private final SqlToolbox lifecycleToolbox;

  /**
   * The construction of these objects in the production code is a bit circuitous.  Specifically, the SqlToolbox
   * looks like it can be normally injected, except it actually expects to be mutated with a SqlEngine before being
   * injected.  This is generally accomplished with Guice, examples of which can be seen in the
   * SqlStatementFactoryModule.
   *
   * @param lifecycleToolbox
   */
  public SqlStatementFactory(SqlToolbox lifecycleToolbox)
  {
    this.lifecycleToolbox = lifecycleToolbox;
  }

  public HttpStatement httpStatement(
      final SqlQuery sqlQuery,
      final HttpServletRequest req
  )
  {
    return new HttpStatement(lifecycleToolbox, sqlQuery, req);
  }

  public DirectStatement directStatement(final SqlQueryPlus sqlRequest)
  {
    return new DirectStatement(lifecycleToolbox, sqlRequest);
  }

  public PreparedStatement preparedStatement(final SqlQueryPlus sqlRequest)
  {
    return new PreparedStatement(lifecycleToolbox, sqlRequest);
  }
}
