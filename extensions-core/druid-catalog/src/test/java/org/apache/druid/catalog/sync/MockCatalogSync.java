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
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import org.apache.druid.catalog.http.CatalogListenerResource;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.TableSpec;
import org.apache.druid.catalog.storage.CatalogStorage;
import org.apache.druid.catalog.storage.CatalogTests;
import org.apache.druid.catalog.sync.MetadataCatalog.CatalogListener;
import org.apache.druid.server.http.catalog.DummyRequest;

import javax.ws.rs.core.MediaType;

import java.io.ByteArrayInputStream;

/**
 * Simulates a network sync from catalog (Coordinator) to consumer (Broker).
 */
public class MockCatalogSync implements CatalogListener
{
  private final CatalogListenerResource listenerResource;
  private final CachedMetadataCatalog catalog;
  private final boolean useSmile;
  private final ObjectMapper smileMapper;
  private final ObjectMapper jsonMapper;

  public MockCatalogSync(
      CatalogStorage storage,
      final ObjectMapper smileMapper,
      final ObjectMapper jsonMapper,
      boolean useSmile
  )
  {
    this.catalog = new CachedMetadataCatalog(storage, storage.schemaRegistry(), jsonMapper);
    this.listenerResource = new CatalogListenerResource(
        catalog,
        smileMapper,
        jsonMapper,
        storage.authorizer().mapper()
    );
    this.useSmile = useSmile;
    this.smileMapper = smileMapper;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public void updated(TableMetadata update)
  {
    doSync(update);
  }

  private void doSync(TableMetadata update)
  {
    byte[] encoded = update.toBytes(useSmile ? smileMapper : jsonMapper);
    listenerResource.syncTable(
        new ByteArrayInputStream(encoded),
        new DummyRequest(
            DummyRequest.POST,
            CatalogTests.SUPER_USER,
            useSmile ? SmileMediaTypes.APPLICATION_JACKSON_SMILE : MediaType.APPLICATION_JSON
        )
    );
  }

  @Override
  public void deleted(TableId tableId)
  {
    TableMetadata spec = TableMetadata.newTable(
        tableId,
        new TableSpec(CatalogUpdateNotifier.TOMBSTONE_TABLE_TYPE, null, null)
    );
    doSync(spec);
  }

  public MetadataCatalog catalog()
  {
    return catalog;
  }
}
