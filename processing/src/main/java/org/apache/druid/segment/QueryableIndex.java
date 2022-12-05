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

package org.apache.druid.segment;

import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.data.Indexed;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Direct interface to memory mapped segments. Not a public API for extensions; site specific queries should be
 * using {@link StorageAdapter}.
 *
 * @see QueryableIndexStorageAdapter for query path adapter
 * @see QueryableIndexIndexableAdapter for indexing path adapter
 */
public interface QueryableIndex extends Closeable
{
  Interval getDataInterval();
  int getNumRows();
  Indexed<String> getAvailableDimensions();
  BitmapFactory getBitmapFactoryForDimensions();
  @Nullable Metadata getMetadata();
  Map<String, DimensionHandler> getDimensionHandlers();

  List<String> getColumnNames();

  @Nullable
  ColumnHolder getColumnHolder(String columnName);

  @Nullable
  default ColumnCapabilities getColumnCapabilities(String column)
  {
    final ColumnHolder columnHolder = getColumnHolder(column);
    if (columnHolder == null) {
      return null;
    }
    return columnHolder.getCapabilities();
  }

  /**
   * The close method shouldn't actually be here as this is nasty. We will adjust it in the future.
   * @throws IOException if an exception was thrown closing the index
   */
  //@Deprecated // This is still required for SimpleQueryableIndex. It should not go away until SimpleQueryableIndex is fixed
  @Override
  void close();

  /**
   * @return true if this index was created from a tombstone or false otherwise
   */
  default boolean isFromTombstone()
  {
    return false;
  }
}
