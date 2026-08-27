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

import org.apache.druid.server.security.AuthenticationResult;

/**
 * Implementation of {@link DruidSchemaCatalogProvider} that always returns the same catalog. Not suitable for
 * security-related tests because the user catalog and escalated catalog are the same; in a real environment these
 * would be different.
 */
public class ConstantDruidSchemaCatalogProvider implements DruidSchemaCatalogProvider
{
  private final DruidSchemaCatalog theCatalog;

  public ConstantDruidSchemaCatalogProvider(DruidSchemaCatalog theCatalog)
  {
    this.theCatalog = theCatalog;
  }

  @Override
  public DruidSchemaCatalog createRootSchema(AuthenticationResult authenticationResult)
  {
    return theCatalog;
  }

  @Override
  public DruidSchemaCatalog createEscalatedRootSchema()
  {
    return theCatalog;
  }
}
