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
import com.google.inject.Provider;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.druid.java.util.common.ISE;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Provides the RootSchema for calcite with
 * - metadata schema disabled because it's not needed
 * - caching disabled because druid's caching is better.
 *
 * All the provided schema are added to the rootSchema.
 */
public class RootSchemaProvider implements Provider<SchemaPlus>
{
  private final Set<NamedSchema> namedSchemas;

  @Inject
  RootSchemaProvider(Set<NamedSchema> namedSchemas)
  {
    this.namedSchemas = namedSchemas;
  }

  @Override
  public SchemaPlus get()
  {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false, false).plus();
    List<String> schemaNames = namedSchemas.stream()
                                           .map(NamedSchema::getSchemaName)
                                           .collect(Collectors.toList());
    Set<String> uniqueSchemaNames = new HashSet<>(schemaNames);
    if (uniqueSchemaNames.size() < schemaNames.size()) {
      throw new ISE("Found multiple schemas registered to the same name. "
                    + "The list of registered schemas are %s", schemaNames);
    }
    namedSchemas.forEach(schema -> rootSchema.add(schema.getSchemaName(), schema.getSchema()));
    return rootSchema;
  }
}
