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

import org.apache.calcite.schema.SchemaPlus;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The Druid 'catalog', containing information about all Druid schemas which are available. This packages both the 'root
 * level' Calcite {@link SchemaPlus} and a map of the {@link NamedSchema} which were used to populate it, keyed by
 * {@link NamedSchema#getSchemaName()}.
 *
 * The {@link #rootSchema} is a top level Calcite schema, which contains all {@link NamedSchema#getSchema()} and
 * {@link InformationSchema} added as sub-schemas, and this class provides convenience methods to do things like
 * fetch a specific {@link SchemaPlus} by name or list the set of all schemas available, and is used during query
 * planning and execution.
 *
 * {@link #namedSchemas} contains all {@link NamedSchema}, which should be everything except {@link InformationSchema}.
 * These are used primarily for {@link #getResourceType(String, String)}, which given the name of a table or function
 * that belongs to some {@link NamedSchema}, lookup the most appropriate value to use for
 * {@link org.apache.druid.server.security.Resource#getType()} to use for authorization.
 */
public class DruidSchemaCatalog
{
  private final SchemaPlus rootSchema;
  private final Map<String, NamedSchema> namedSchemas;

  public DruidSchemaCatalog(
      SchemaPlus rootSchema,
      Map<String, NamedSchema> schemas
  )
  {
    this.rootSchema = rootSchema;
    this.namedSchemas = schemas;
  }

  /**
   * Root calcite schema, used to plan and execute queries
   */
  public SchemaPlus getRootSchema()
  {
    return rootSchema;
  }

  /**
   * Get all {@link NamedSchema} which belong to the Druid catalog
   */
  public Map<String, NamedSchema> getNamedSchemas()
  {
    return namedSchemas;
  }

  /**
   * Get a {@link NamedSchema} by {@link NamedSchema#getSchemaName()}
   */
  public NamedSchema getNamedSchema(String schemaName)
  {
    return namedSchemas.get(schemaName);
  }

  /**
   * Get a specific {@link SchemaPlus} by {@link NamedSchema#getSchemaName()}
   */
  public SchemaPlus getSubSchema(String name)
  {
    return rootSchema.getSubSchema(name);
  }

  /**
   * Get all sub-schemas defined on {@link #rootSchema}
   */
  public Set<String> getSubSchemaNames()
  {
    return rootSchema.getSubSchemaNames();
  }

  /**
   * Given the name of a {@link NamedSchema} and the name of a table or function that belongs to that schema, return
   * the appropriate value to use for {@link org.apache.druid.server.security.Resource#getType()} during authorization
   */
  @Nullable
  public String getResourceType(String schema, String resourceName)
  {
    if (namedSchemas.containsKey(schema)) {
      return namedSchemas.get(schema).getSchemaResourceType(resourceName);
    }
    return null;
  }


  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DruidSchemaCatalog that = (DruidSchemaCatalog) o;
    return rootSchema.equals(that.rootSchema) && namedSchemas.equals(that.namedSchemas);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(rootSchema, namedSchemas);
  }

  @Override
  public String toString()
  {
    return "DruidSchemaCatalog{" +
           "schemas=" + getSubSchemaNames() +
           '}';
  }
}
