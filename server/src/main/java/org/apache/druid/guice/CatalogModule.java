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

package org.apache.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Module;
import org.apache.druid.catalog.CatalogStorage;
import org.apache.druid.catalog.CatalogUpdateNotifier;
import org.apache.druid.catalog.MetastoreManager;
import org.apache.druid.catalog.MetastoreManagerImpl;
import org.apache.druid.catalog.SchemaRegistry;
import org.apache.druid.catalog.SchemaRegistryImpl;
import org.apache.druid.metadata.catalog.CatalogManager;
import org.apache.druid.metadata.catalog.SQLCatalogManager;
import org.apache.druid.server.http.CatalogResource;

/**
 * Configures the catalog database on the Coordinator, along
 * with its REST resource for CRUD updates and the notifier
 * for push updates.
 */
public class CatalogModule implements Module
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
        .bind(MetastoreManager.class)
        .to(MetastoreManagerImpl.class)
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
}
