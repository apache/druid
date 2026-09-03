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
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

@LazySingleton
public class DruidSchemaCatalogProviderImpl implements DruidSchemaCatalogProvider
{
  private static final String INFORMATION_SCHEMA_NAME = "INFORMATION_SCHEMA";

  private final Set<NamedSchema> namedSchemas;
  private final Set<SchemaProvider> schemaProviders;
  private final DruidOperatorTable operatorTable;
  private final AuthorizerMapper authorizerMapper;
  private final Escalator escalator;

  @Inject
  public DruidSchemaCatalogProviderImpl(
      Set<NamedSchema> namedSchemas,
      Set<SchemaProvider> schemaProviders,
      DruidOperatorTable operatorTable,
      AuthorizerMapper authorizerMapper,
      Escalator escalator
  )
  {
    this.namedSchemas = namedSchemas;
    this.schemaProviders = schemaProviders;
    this.operatorTable = operatorTable;
    this.authorizerMapper = authorizerMapper;
    this.escalator = escalator;
  }

  @Override
  public DruidSchemaCatalog createRootSchema(final AuthenticationResult authenticationResult)
  {
    // Metadata schema is disabled because it is not needed. Caching is disabled because we want to avoid
    // materializing every table, as Calcite's caching schema would do.
    final SchemaPlus rootSchema = CalciteSchema.createRootSchema(false, false).plus();
    final Map<String, NamedSchema> allSchemas = new TreeMap<>();

    for (NamedSchema schema : namedSchemas) {
      if (allSchemas.putIfAbsent(schema.getSchemaName(), schema) != null) {
        throw new ISE("Schema name conflict for[%s]", schema.getSchemaName());
      }
    }

    for (SchemaProvider schemaProvider : schemaProviders) {
      for (NamedSchema schema : schemaProvider.getSchemas(authenticationResult)) {
        if (allSchemas.putIfAbsent(schema.getSchemaName(), schema) != null) {
          throw new ISE("Schema name conflict for[%s]", schema.getSchemaName());
        }
      }
    }

    if (allSchemas.containsKey(INFORMATION_SCHEMA_NAME)) {
      throw new ISE("Cannot have schema named[%s]", INFORMATION_SCHEMA_NAME);
    }

    // Add allSchemas to the rootSchema.
    for (final NamedSchema namedSchema : allSchemas.values()) {
      rootSchema.add(namedSchema.getSchemaName(), namedSchema.getSchema());
    }

    final DruidSchemaCatalog schemaCatalog = new DruidSchemaCatalog(rootSchema, allSchemas);

    // One more schema to add: INFORMATION_SCHEMA.
    rootSchema.add(
        INFORMATION_SCHEMA_NAME,
        new InformationSchema(
            schemaCatalog,
            operatorTable,
            authorizerMapper,
            authenticationResult
        )
    );

    return schemaCatalog;
  }

  @Override
  public DruidSchemaCatalog createEscalatedRootSchema()
  {
    return createRootSchema(escalator.createEscalatedAuthenticationResult());
  }
}
