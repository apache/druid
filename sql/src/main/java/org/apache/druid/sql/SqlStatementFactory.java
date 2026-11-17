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

import org.apache.druid.server.initialization.jetty.RequestHeaderContextFilterHolder;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.Map;

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
      final SqlQueryPlus sqlQueryPlus,
      final HttpServletRequest req
  )
  {
    final Map<String, String> headers = RequestHeaderContextFilterHolder.contextFor(req);
    if (headers.isEmpty()) {
      return new HttpStatement(lifecycleToolbox, sqlQueryPlus, req);
    }

    // Preserve which keys were supplied by the client, including the captured headers.
    final Map<String, Object> userContext = new HashMap<>();
    sqlQueryPlus.authContextKeys().forEach(key -> userContext.put(key, sqlQueryPlus.context().get(key)));
    userContext.putAll(headers);
    return new HttpStatement(
        lifecycleToolbox,
        sqlQueryPlus.withContext(sqlQueryPlus.context(), userContext),
        req
    );
  }

  public DirectStatement directStatement(final SqlQueryPlus sqlRequest)
  {
    return new DirectStatement(lifecycleToolbox, sqlRequest, null);
  }

  public DirectStatement directStatement(final SqlQueryPlus sqlRequest, @Nullable final String remoteAddress)
  {
    return new DirectStatement(lifecycleToolbox, sqlRequest, remoteAddress);
  }

  public PreparedStatement preparedStatement(final SqlQueryPlus sqlRequest)
  {
    return new PreparedStatement(lifecycleToolbox, sqlRequest, null);
  }

  public PreparedStatement preparedStatement(final SqlQueryPlus sqlRequest, @Nullable final String remoteAddress)
  {
    return new PreparedStatement(lifecycleToolbox, sqlRequest, remoteAddress);
  }
}
