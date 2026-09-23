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

package org.apache.druid.sql.calcite.schema;

import com.google.inject.Binder;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.guice.SqlBindings;

import java.util.List;

/**
 * Provides {@link NamedSchema} in a user-aware way. Bind with {@link SqlBindings#addSchemaProvider(Binder, Class)}.
 */
public interface SchemaProvider
{
  /**
   * Return a list of {@link NamedSchema} for the provided user. These schemas contain the objects that are
   * visible to the provided user. The user is not necessarily authorized to perform all operations, or even
   * any operations, on these objects. Authorization must be checked separately.
   *
   * <p>Schema providers that produce authorizable tables must check the value of
   * {@link PlannerConfig#isAuthorizeTableVisibility()} and use this to determine whether to place unauthorized
   * tables in the returned schemas.
   *
   * @param authenticationResult identity of the current user
   */
  List<NamedSchema> getSchemas(AuthenticationResult authenticationResult);
}
