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

import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.http.SqlQuery;

import javax.servlet.http.HttpServletRequest;
import java.util.Set;
import java.util.function.Function;

/**
 * SQL statement lifecycle for the HTTP endpoint. The request thread
 * creates the object and calls {@link #execute()}. The response thread
 * reads results and inspects the statement contents to emit logs and
 * metrics. The object is transferred between threads, with no overlapping
 * access.
 * <p>
 * The key extension of an HTTP statement is the use of the HTTP request
 * for authorization.
 */
public class HttpStatement extends DirectStatement
{
  private final HttpServletRequest req;

  public HttpStatement(
      final SqlToolbox lifecycleToolbox,
      final SqlQuery sqlQuery,
      final HttpServletRequest req
  )
  {
    super(
        lifecycleToolbox,
        SqlQueryPlus.builder(sqlQuery)
          .auth(AuthorizationUtils.authenticationResultFromRequest(req))
          .build(),
        req.getRemoteAddr()
    );
    this.req = req;
  }

  @Override
  protected Function<Set<ResourceAction>, Access> authorizer()
  {
    return resourceActions ->
      AuthorizationUtils.authorizeAllResourceActions(
          req,
          resourceActions,
          sqlToolbox.plannerFactory.getAuthorizerMapper()
    );
  }
}
