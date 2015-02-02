/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */


package io.druid.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.inject.Inject;
import com.metamx.common.lifecycle.Lifecycle;


public class SQLMetadataSegmentManagerProvider implements MetadataSegmentManagerProvider
{
  private final ObjectMapper jsonMapper;
  private final Supplier<MetadataSegmentManagerConfig> config;
  private final Supplier<MetadataStorageTablesConfig> storageConfig;
  private final SQLMetadataConnector connector;
  private final Lifecycle lifecycle;

  @Inject
  public SQLMetadataSegmentManagerProvider(
      ObjectMapper jsonMapper,
      Supplier<MetadataSegmentManagerConfig> config,
      Supplier<MetadataStorageTablesConfig> storageConfig,
      SQLMetadataConnector connector,
      Lifecycle lifecycle
  )
  {
    this.jsonMapper = jsonMapper;
    this.config = config;
    this.storageConfig = storageConfig;
    this.connector = connector;
    this.lifecycle = lifecycle;
  }

  @Override
  public MetadataSegmentManager get()
  {
    lifecycle.addHandler(
        new Lifecycle.Handler()
        {
          @Override
          public void start() throws Exception
          {
            connector.createSegmentTable();
          }

          @Override
          public void stop()
          {

          }
        }
    );

    return new SQLMetadataSegmentManager(
        jsonMapper,
        config,
        storageConfig,
        connector
    );
  }
}
