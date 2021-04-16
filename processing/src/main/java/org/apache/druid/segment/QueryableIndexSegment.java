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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

/**
 *
 */
public class QueryableIndexSegment implements Segment
{
  private final Supplier<QueryableIndex> indexSupplier;
  private final Supplier<QueryableIndexStorageAdapter> queryableIndexStorageAdapterSupplier;
  private final SegmentId segmentId;

  public QueryableIndexSegment(QueryableIndex index, final SegmentId segmentId)
  {
    this.indexSupplier = new Supplier<QueryableIndex>()
    {
      @Override
      public QueryableIndex get()
      {
        return index;
      }
    };
    this.queryableIndexStorageAdapterSupplier = Suppliers.memoize(new Supplier<QueryableIndexStorageAdapter>()
    {
      @Override
      public QueryableIndexStorageAdapter get()
      {
        return new QueryableIndexStorageAdapter(index);
      }
    });
    this.segmentId = segmentId;
  }

  public QueryableIndexSegment(Supplier<QueryableIndex> indexSupplier, final SegmentId segmentId)
  {
    this.indexSupplier = indexSupplier;
    this.queryableIndexStorageAdapterSupplier = Suppliers.memoize(new Supplier<QueryableIndexStorageAdapter>()
    {
      @Override
      public QueryableIndexStorageAdapter get()
      {
        return new QueryableIndexStorageAdapter(indexSupplier.get());
      }
    });
    this.segmentId = segmentId;
  }

  @Override
  public SegmentId getId()
  {
    return segmentId;
  }

  @Override
  public Interval getDataInterval()
  {
    return indexSupplier.get().getDataInterval();
  }

  @Override
  public QueryableIndex asQueryableIndex()
  {
    return indexSupplier.get();
  }

  @Override
  public StorageAdapter asStorageAdapter()
  {
    return queryableIndexStorageAdapterSupplier.get();
  }

  @Override
  public void close()
  {
    // this is kinda nasty
    indexSupplier.get().close();
  }
}
