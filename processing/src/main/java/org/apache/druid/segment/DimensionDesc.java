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

import com.google.common.base.Preconditions;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import javax.annotation.Nullable;

public final class DimensionDesc
{
  private final int index;
  private final String name;
  private final ColumnCapabilitiesImpl capabilities;
  private final DimensionHandler handler;

  // Indexer is initialized lazily because it's used in only IncrementalIndex
  @Nullable
  private DimensionIndexer indexer = null;

  @Nullable
  private DimensionMerger merger = null;

  public DimensionDesc(int index, String name, ColumnCapabilitiesImpl capabilities, DimensionHandler handler)
  {
    this.index = index;
    this.name = name;
    this.capabilities = capabilities;
    this.handler = handler;
  }

  public int getIndex()
  {
    return index;
  }

  public String getName()
  {
    return name;
  }

  public ColumnCapabilitiesImpl getCapabilities()
  {
    return capabilities;
  }

  public DimensionHandler getHandler()
  {
    return handler;
  }

  public DimensionIndexer makeOrGetIndexer()
  {
    if (indexer == null) {
      indexer = handler.makeIndexer();
    }
    return indexer;
  }

  public DimensionMerger makeMerger(IndexSpec indexSpec, SegmentWriteOutMedium medium, ProgressIndicator progress)
  {
    this.merger = handler.makeMerger(indexSpec, medium, capabilities, progress);
    return merger;
  }

  public DimensionMerger getNonNullMerger()
  {
    return Preconditions.checkNotNull(merger, "dimensionMerger");
  }
}
