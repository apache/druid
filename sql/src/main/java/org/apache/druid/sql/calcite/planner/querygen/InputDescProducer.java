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

import com.google.errorprone.annotations.Immutable;
import org.apache.druid.query.DataSource;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;

import java.util.List;

/**
 * Abstracts away non-trivial input operation handlings between {@link DataSource}s.
 *
 * Example: TableScan ; Union; Join.
 *
 * FIXME: this class is handy; but it does created a small confusion (for me at least)
 * there are 2 distinct usage of datasources - which are similar but also different:
 *   * for a given query an input is a datasource - that is: the source of the query
 *   * for a given join or more complicated datasource (which will be a base for a query) the inputs are also datasources
 *     * however these datasources are "different" in the sence that they may cover an underlying query or similar
 *     * the Join datasource in particular does more than just "covering" multiple datasource input
 *       and thus may use virtual columns as well
 */
public interface InputDescProducer
{
  /**
   * Utility class to input related things details.
   *
   * Main reason to have this was that {@link DataSource} doesn't contain the {@link RowSignature}.
   */
  @Immutable
  class InputDesc
  {
    public final DataSource dataSource;
    public final RowSignature rowSignature;
    public final VirtualColumnRegistry virtualColumnRegistry;

    public InputDesc(DataSource dataSource, RowSignature rowSignature)
    {
      this(dataSource, rowSignature, null);
    }

    public InputDesc(DataSource dataSource, RowSignature rowSignature, VirtualColumnRegistry virtualColumnRegistry)
    {
      this.dataSource = dataSource;
      this.rowSignature = rowSignature;
      this.virtualColumnRegistry = virtualColumnRegistry;
    }
  }

  InputDesc getInputDesc(PlannerContext plannerContext, List<InputDesc> inputs);
}
