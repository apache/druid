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

package org.apache.druid.catalog.sync;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.catalog.http.CatalogListenerResource;
import org.apache.druid.catalog.storage.CatalogStorage;

/**
 * Simulates a network sync from catalog (Coordinator) to consumer (Broker).
 */
public class MockCatalogSync implements CatalogUpdateListener
{
  private final CatalogListenerResource listenerResource;
  private final CachedMetadataCatalog catalog;

  public MockCatalogSync(
      final CatalogStorage storage,
      final ObjectMapper jsonMapper
  )
  {
    this.catalog = new CachedMetadataCatalog(storage, storage.schemaRegistry(), jsonMapper);
    this.listenerResource = new CatalogListenerResource(catalog);
  }

  @Override
  public void updated(UpdateEvent update)
  {
    doSync(update);
  }

  private void doSync(UpdateEvent event)
  {
    listenerResource.syncTable(event);
  }

  public MetadataCatalog catalog()
  {
    return catalog;
  }

  @Override
  public void flush()
  {
  }

  @Override
  public void resync()
  {
  }
}
