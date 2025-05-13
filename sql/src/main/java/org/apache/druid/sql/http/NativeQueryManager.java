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

package org.apache.druid.sql.http;

import com.google.inject.Inject;
import org.apache.druid.error.DruidException;
import org.apache.druid.server.security.AuthorizationResult;
import org.apache.druid.sql.SqlLifecycleManager;

import javax.ws.rs.core.Response;
import java.util.List;
import java.util.function.Function;

public class NativeQueryManager implements QueryManager
{
  private final SqlLifecycleManager sqlLifecycleManager;

  @Inject
  public NativeQueryManager(SqlLifecycleManager sqlLifecycleManager)
  {
    this.sqlLifecycleManager = sqlLifecycleManager;
  }

  @Override
  public Response cancelQuery(String sqlQueryId, Function<List<SqlLifecycleManager.Cancelable>, AuthorizationResult> authFunction)
  {
    List<SqlLifecycleManager.Cancelable> lifecycles = sqlLifecycleManager.getAll(sqlQueryId);
    if (lifecycles.isEmpty()) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    final AuthorizationResult authResult = authFunction.apply(lifecycles);

    if (authResult.allowAccessWithNoRestriction()) {
      // should remove only the lifecycles in the snapshot.
      sqlLifecycleManager.removeAll(sqlQueryId, lifecycles);
      lifecycles.forEach(SqlLifecycleManager.Cancelable::cancel);
      return Response.status(Response.Status.ACCEPTED).build();
    } else {
      return Response.status(Response.Status.FORBIDDEN).build();
    }
  }

  @Override
  public List<QueryInfo> getRunningQueries(boolean selfOnly)
  {
    throw DruidException.forPersona(DruidException.Persona.USER)
                        .ofCategory(DruidException.Category.UNSUPPORTED)
                        .build("getRunningQueries is not supported for native queries.");
  }
}
