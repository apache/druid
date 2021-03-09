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

package org.apache.druid.segment.join;

import org.apache.druid.com.google.common.collect.Iterators;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.GlobalTableDataSource;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.segment.join.table.IndexedTable;
import org.apache.druid.segment.join.table.IndexedTableJoinable;
import org.apache.druid.segment.join.table.ReferenceCountingIndexedTable;
import org.apache.druid.server.SegmentManager;

import java.util.Iterator;
import java.util.Optional;

public class BroadcastTableJoinableFactory implements JoinableFactory
{
  private final SegmentManager segmentManager;

  @Inject
  public BroadcastTableJoinableFactory(SegmentManager segmentManager)
  {
    this.segmentManager = segmentManager;
  }

  @Override
  public boolean isDirectlyJoinable(DataSource dataSource)
  {
    GlobalTableDataSource broadcastDatasource = (GlobalTableDataSource) dataSource;
    return broadcastDatasource != null && segmentManager.hasIndexedTables(broadcastDatasource.getName());
  }

  @Override
  public Optional<Joinable> build(
      DataSource dataSource,
      JoinConditionAnalysis condition
  )
  {
    if (!condition.canHashJoin()) {
      return Optional.empty();
    }
    return getOnlyIndexedTable(dataSource).map(IndexedTableJoinable::new);
  }

  @Override
  public Optional<byte[]> computeJoinCacheKey(DataSource dataSource, JoinConditionAnalysis condition)
  {
    if (!condition.canHashJoin()) {
      return Optional.empty();
    }
    return getOnlyIndexedTable(dataSource).filter(IndexedTable::isCacheable).map(IndexedTable::computeCacheKey);
  }

  private Optional<ReferenceCountingIndexedTable> getOnlyIndexedTable(DataSource dataSource)
  {
    GlobalTableDataSource broadcastDataSource = (GlobalTableDataSource) dataSource;
    DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(dataSource);
    return segmentManager.getIndexedTables(analysis).flatMap(tables -> {
      Iterator<ReferenceCountingIndexedTable> tableIterator = tables.iterator();
      if (!tableIterator.hasNext()) {
        return Optional.empty();
      }
      try {
        return Optional.of(Iterators.getOnlyElement(tableIterator));
      }
      catch (IllegalArgumentException iae) {
        throw new ISE(
            "Currently only single segment datasources are supported for broadcast joins, dataSource[%s] has multiple segments. Reingest the data so that it is entirely contained within a single segment to use in JOIN queries.",
            broadcastDataSource.getName()
        );
      }
    });
  }
}
