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

package org.apache.druid.sql.calcite.planner;

import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.druid.sql.calcite.schema.DruidCalciteSchema;

import java.util.Set;

/**
 * Provides the RootSchema for calcite with
 * - metadata schema disabled because it's not needed
 * - caching disabled because druid's caching is better.
 *
 * All the provided schema are added to the rootSchema.
 */
public class RootSchemaProvider implements Provider<SchemaPlus>
{
  private final Set<DruidCalciteSchema> calciteSchemas;

  @Inject
  RootSchemaProvider(Set<DruidCalciteSchema> calciteSchemas)
  {
    this.calciteSchemas = calciteSchemas;
  }

  @Override
  public SchemaPlus get()
  {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false, false).plus();
    calciteSchemas.forEach(schema -> rootSchema.add(schema.getSchemaName(), schema.getSchema()));
    return rootSchema;
  }
}
