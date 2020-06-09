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
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.sql.guice.SqlBindings;

/**
 * The module responsible for providing bindings to Calcite schemas.
 */
public class DruidCalciteSchemaModule implements Module
{
  private static final String DRUID_SCHEMA_NAME = "druid";
  private static final String INFORMATION_SCHEMA_NAME = "INFORMATION_SCHEMA";
  static final String INCOMPLETE_SCHEMA = "INCOMPLETE_SCHEMA";

  @Override
  public void configure(Binder binder)
  {
    binder.bind(String.class).annotatedWith(DruidSchemaName.class).toInstance(DRUID_SCHEMA_NAME);

    // Should only be used by the information schema
    binder.bind(SchemaPlus.class)
          .annotatedWith(Names.named(INCOMPLETE_SCHEMA))
          .toProvider(RootSchemaProvider.class)
          .in(Scopes.SINGLETON);

    // DruidSchema needs to listen to changes for incoming segments
    LifecycleModule.register(binder, DruidSchema.class);
    binder.bind(SystemSchema.class).in(Scopes.SINGLETON);
    binder.bind(InformationSchema.class).in(Scopes.SINGLETON);
    binder.bind(LookupSchema.class).in(Scopes.SINGLETON);

    // Binder to inject different schema to Calcite
    SqlBindings.addSchema(binder, NamedDruidSchema.class);
    SqlBindings.addSchema(binder, NamedSystemSchema.class);
    SqlBindings.addSchema(binder, NamedLookupSchema.class);
  }

  @Provides
  @Singleton
  private SchemaPlus getRootSchema(@Named(INCOMPLETE_SCHEMA) SchemaPlus rootSchema, InformationSchema informationSchema)
  {
    rootSchema.add(INFORMATION_SCHEMA_NAME, informationSchema);
    return rootSchema;
  }
}
