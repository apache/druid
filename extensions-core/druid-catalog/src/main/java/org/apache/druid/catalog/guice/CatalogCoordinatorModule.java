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

package org.apache.druid.catalog.guice;

import com.fasterxml.jackson.databind.Module;
import com.google.inject.Binder;
import org.apache.druid.catalog.http.CatalogResource;
import org.apache.druid.catalog.model.SchemaRegistry;
import org.apache.druid.catalog.model.SchemaRegistryImpl;
import org.apache.druid.catalog.storage.CatalogStorage;
import org.apache.druid.catalog.storage.MetadataStorageManager;
import org.apache.druid.catalog.storage.sql.CatalogManager;
import org.apache.druid.catalog.storage.sql.SQLCatalogManager;
import org.apache.druid.catalog.sync.CatalogUpdateNotifier;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.LoadScope;
import org.apache.druid.initialization.DruidModule;

import java.util.Collections;
import java.util.List;

/**
 * Configures the catalog database on the Coordinator, along
 * with its REST resource for CRUD updates and the notifier
 * for push updates.
 */
@LoadScope(roles = NodeRole.COORDINATOR_JSON_NAME)
public class CatalogCoordinatorModule implements DruidModule
{
  @Override
  public void configure(Binder binder)
  {
    // Database layer: only the SQL version is supported at present.
    binder
        .bind(CatalogManager.class)
        .to(SQLCatalogManager.class)
        .in(LazySingleton.class);

    // Storage abstraction used by the REST API, sits on top of the
    // database layer.
    binder
        .bind(CatalogStorage.class)
        .in(LazySingleton.class);
    binder
        .bind(MetadataStorageManager.class)
        .in(LazySingleton.class);

    // At present, the set of schemas is fixed.
    binder
        .bind(SchemaRegistry.class)
        .to(SchemaRegistryImpl.class)
        .in(LazySingleton.class);

    // Push update notifier, which is lifecycle managed. No references,
    // so force Guice to create the instance. (Lifecycle will also, if
    // Guice hasn't done so.)
    binder
        .bind(CatalogUpdateNotifier.class)
        .in(ManageLifecycle.class);
    LifecycleModule.register(binder, CatalogUpdateNotifier.class);

    // Public REST API and private cache sync API.
    Jerseys.addResource(binder, CatalogResource.class);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.emptyList();
  }
}

