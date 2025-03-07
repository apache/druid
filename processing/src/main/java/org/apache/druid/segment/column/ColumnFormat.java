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

package org.apache.druid.segment.column;

import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.segment.DimensionHandler;

import javax.annotation.Nullable;

/**
 * Abstraction for the physical storage components of a column, allowing us to decouple a columns logical type from
 * how it is actually stored in a segment. Also provides methods to create {@link DimensionHandler} and
 * {@link DimensionSchema} for creating new columns with identical physical storage components.
 */
public interface ColumnFormat
{
  ColumnType getLogicalType();

  /**
   * Convert physical column details to generic {@link ColumnCapabilities} for query time analysis
   */
  ColumnCapabilities toColumnCapabilities();

  /**
   * Create a {@link DimensionHandler} which can be used to create indexers and mergers to produce a new column with
   * this format
   */
  DimensionHandler getColumnHandler(String columnName);

  /**
   * Creates a {@link DimensionSchema} which can be used to create new segment schemas for ingestion jobs to create
   * columns with this format
   */
  DimensionSchema getColumnSchema(String columnName);

  /**
   * Create a new {@link ColumnFormat} from this format and another format, merging implementation specific physical
   * storage details as appropriate to determine what the new format should be.
   */
  ColumnFormat merge(@Nullable ColumnFormat otherFormat);
}
