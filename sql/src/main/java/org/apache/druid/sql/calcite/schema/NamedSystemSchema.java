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

import com.google.inject.Inject;
import org.apache.calcite.schema.Schema;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.planner.PlannerConfig;

import javax.annotation.Nullable;

/**
 * The schema for Druid system tables to be accessible via SQL.
 */
public class NamedSystemSchema implements NamedSchema
{
  public static final String NAME = "sys";

  private final SystemSchema systemSchema;
  private final PlannerConfig plannerConfig;

  @Inject
  public NamedSystemSchema(PlannerConfig plannerConfig, SystemSchema systemSchema)
  {
    this.plannerConfig = plannerConfig;
    this.systemSchema = systemSchema;
  }

  @Override
  public String getSchemaName()
  {
    return NAME;
  }

  @Override
  public Schema getSchema()
  {
    return systemSchema;
  }

  @Nullable
  @Override
  public String getSchemaResourceType(String resourceName)
  {
    if (plannerConfig.isAuthorizeSystemTablesDirectly()) {
      return ResourceType.SYSTEM_TABLE;
    }
    return null;
  }
}
