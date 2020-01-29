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
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.sql.guice.SqlBindings;

/**
 * The module responsible for providing bindings
 */
public class DruidCalciteSchemaModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    // DruidSchema needs to listen to changes for incoming segments
    LifecycleModule.register(binder, DruidSchema.class);

    // Binder to inject different schema to Calcite
    Multibinder<DruidCalciteSchema> schemaMultibinder = SqlBindings.calciteSchemaBinder(binder);
    schemaMultibinder.addBinding().to(DruidSchema.class).in(Scopes.SINGLETON);
    schemaMultibinder.addBinding().to(InformationSchema.class).in(Scopes.SINGLETON);
    schemaMultibinder.addBinding().to(SystemSchema.class).in(Scopes.SINGLETON);
  }
}
