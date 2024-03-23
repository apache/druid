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

package org.apache.druid.sql.calcite.planner.querygen;

import org.apache.druid.query.DataSource;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;

import java.util.List;

/**
 * Abstracts away non-trivial input operation handlings between {@link DataSource}s.
 *
 * Example: TableScan ; Union; Join.
 */
public interface SourceDescProducer
{
  /**
   * Utility class to input related things details.
   *
   * Main reason to have this was that {@link DataSource} doesn't contain the {@link RowSignature}.
   */
  class SourceDesc
  {
    public final DataSource dataSource;
    public final RowSignature rowSignature;
    public final VirtualColumnRegistry virtualColumnRegistry;

    public SourceDesc(DataSource dataSource, RowSignature rowSignature)
    {
      this(dataSource, rowSignature, null);
    }

    public SourceDesc(DataSource dataSource, RowSignature rowSignature, VirtualColumnRegistry virtualColumnRegistry)
    {
      this.dataSource = dataSource;
      this.rowSignature = rowSignature;
      this.virtualColumnRegistry = virtualColumnRegistry;
    }
  }

  SourceDesc getSourceDesc(PlannerContext plannerContext, List<SourceDesc> sources);
}
