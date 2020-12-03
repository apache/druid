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

package org.apache.druid.mapStringString;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.data.GenericIndexed;

import java.io.IOException;
import java.nio.ByteBuffer;

public class MapStringStringColumnSupplier implements Supplier<MapStringStringComplexColumn>
{
  private final MapStringStringColumnMetadata metadata;
  private final GenericIndexed<String> keys;

  private final ColumnConfig columnConfig;
  private final SmooshedFileMapper fileMapper;

  public MapStringStringColumnSupplier(
      ByteBuffer bb,
      ColumnBuilder columnBuilder,
      ColumnConfig columnConfig,
      ObjectMapper jsonMapper
  )
  {
    byte version = bb.get();
    if (version != MapStringStringColumnSerializer.VERSION) {
      throw new IAE("Unknown version [%s].", version);
    }

    try {
      metadata = jsonMapper.readValue(
          MapStringStringColumnSerializer.SERIALIZER_UTILS.readString(bb),
          MapStringStringColumnMetadata.class
      );

      keys = GenericIndexed.read(bb, GenericIndexed.STRING_STRATEGY);
    }
    catch (IOException ex) {
      throw new RE(ex, "Failed to read metadata.");
    }

    fileMapper = Preconditions.checkNotNull(columnBuilder.getFileMapper(), "Null fileMapper");

    this.columnConfig = columnConfig;
  }

  @Override
  public MapStringStringComplexColumn get()
  {
    return new MapStringStringComplexColumn(
        metadata,
        keys,
        columnConfig,
        fileMapper
    );
  }
}
