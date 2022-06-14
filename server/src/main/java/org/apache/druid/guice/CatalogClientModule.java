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
import org.apache.druid.catalog.CachedMetadataCatalog;
import org.apache.druid.catalog.CatalogClient;
import org.apache.druid.catalog.MetadataCatalog;
import org.apache.druid.catalog.MetadataCatalog.CatalogListener;
import org.apache.druid.catalog.MetadataCatalog.CatalogSource;
import org.apache.druid.catalog.SchemaRegistry;
import org.apache.druid.catalog.SchemaRegistryImpl;
import org.apache.druid.server.http.CatalogListenerResource;

/**
 * Configures the metadata catalog on the Broker to use a cache
 * and network communications for pull and push updates.
 */
public class CatalogClientModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    // The Broker (client) uses a cached metadata catalog.
    binder
        .bind(CachedMetadataCatalog.class)
        .in(LazySingleton.class);

    // Broker code accesses he catalog through the
    // MetadataCatalog interface.
    binder
        .bind(MetadataCatalog.class)
        .to(CachedMetadataCatalog.class)
        .in(LazySingleton.class);

    // The cached metadata catalog needs a "pull" source,
    // which is the network client.
    binder
        .bind(CatalogSource.class)
        .to(CatalogClient.class)
        .in(LazySingleton.class);

    // The cached metadata catalog is the listener for
    // "push" events.
    binder
        .bind(CatalogListener.class)
        .to(CachedMetadataCatalog.class)
        .in(LazySingleton.class);

    // At present, the set of schemas is fixed.
    binder
        .bind(SchemaRegistry.class)
        .to(SchemaRegistryImpl.class)
        .in(LazySingleton.class);

    // The listener resource sends to the catalog
    // listener (the cached catalog.)
    Jerseys.addResource(binder, CatalogListenerResource.class);
  }
}
