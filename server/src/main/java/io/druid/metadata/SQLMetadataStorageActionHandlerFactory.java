/*
 * Druid - a distributed column store.
 * Copyright (C) 2014  Metamarkets Group Inc.
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
import com.google.inject.Inject;

public class SQLMetadataStorageActionHandlerFactory implements MetadataStorageActionHandlerFactory
{
  private final SQLMetadataConnector connector;
  private final MetadataStorageTablesConfig config;
  private final ObjectMapper jsonMapper;

  @Inject
  public SQLMetadataStorageActionHandlerFactory(
      SQLMetadataConnector connector,
      MetadataStorageTablesConfig config,
      ObjectMapper jsonMapper
  )
  {
    this.connector = connector;
    this.config = config;
    this.jsonMapper = jsonMapper;
  }

  public <A,B,C,D> MetadataStorageActionHandler<A,B,C,D> create(
      final String entryType,
      MetadataStorageActionHandlerTypes<A,B,C,D> payloadTypes
  )
  {
    return new SQLMetadataStorageActionHandler<>(
        connector,
        jsonMapper,
        payloadTypes,
        entryType,
        config.getEntryTable(entryType),
        config.getLogTable(entryType),
        config.getLockTable(entryType)
    );
  }
}
