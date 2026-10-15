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

import org.apache.druid.segment.column.ColumnCapabilities;

import javax.annotation.Nullable;

/**
 * Implementation of {@link PhysicalSegmentInspector} that runs on non-fully-downloaded segments.
 * Avoids triggering any fetches, just answers to the best of its ability.
 */
public record PartialQueryableIndexPhysicalSegmentInspector(PartialQueryableIndex index)
    implements PhysicalSegmentInspector
{
  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return index.getColumnCapabilities(column);
  }

  @Nullable
  @Override
  public Comparable getMinValue(String column)
  {
    return null;
  }

  @Nullable
  @Override
  public Comparable getMaxValue(String column)
  {
    return null;
  }

  @Override
  public int getDimensionCardinality(String column)
  {
    return DimensionDictionarySelector.CARDINALITY_UNKNOWN;
  }

  @Override
  @Nullable
  public Metadata getMetadata()
  {
    return index.getMetadata();
  }

  @Override
  public int getNumRows()
  {
    return index.getNumRows();
  }
}
