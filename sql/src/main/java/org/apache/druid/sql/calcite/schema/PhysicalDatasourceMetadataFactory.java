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

package org.apache.druid.sql.calcite.schema;

import com.google.inject.Inject;
import org.apache.druid.query.GlobalTableDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.sql.calcite.table.DatasourceTable.PhysicalDatasourceMetadata;

/**
 * Builds {@link PhysicalDatasourceMetadata} for a dataSource, including information about its schema,
 * joinability, and broadcast status.
 */
public class PhysicalDatasourceMetadataFactory
{
  private final JoinableFactory joinableFactory;
  private final SegmentManager segmentManager;

  @Inject
  public PhysicalDatasourceMetadataFactory(JoinableFactory joinableFactory, SegmentManager segmentManager)
  {
    this.joinableFactory = joinableFactory;
    this.segmentManager = segmentManager;
  }

  /**
   * Builds physical metadata for the given data source.
   *
   * @param dataSource name of the dataSource
   * @param rowSignature schema of the dataSource
   *
   * @return PhysicalDatasourceMetadata which includes information about schema, joinability and broadcast status
   */
  PhysicalDatasourceMetadata build(final String dataSource, final RowSignature rowSignature)
  {
    final TableDataSource tableDataSource;

    // to be a GlobalTableDataSource instead of a TableDataSource, it must appear on all servers (inferred by existing
    // in the segment cache, which in this case belongs to the broker meaning only broadcast segments live here)
    // to be joinable, it must be possibly joinable according to the factory. we only consider broadcast datasources
    // at this time, and isGlobal is currently strongly coupled with joinable, so only make a global table datasource
    // if also joinable
    final GlobalTableDataSource maybeGlobal = new GlobalTableDataSource(dataSource);
    final boolean isJoinable = joinableFactory.isDirectlyJoinable(maybeGlobal);
    final boolean isBroadcast = segmentManager.getDataSourceNames().contains(dataSource);
    if (isBroadcast && isJoinable) {
      tableDataSource = maybeGlobal;
    } else {
      tableDataSource = new TableDataSource(dataSource);
    }
    return new PhysicalDatasourceMetadata(
        tableDataSource,
        rowSignature,
        isJoinable,
        isBroadcast
    );
  }
}
