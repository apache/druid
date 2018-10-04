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

package org.apache.druid.query;

import org.apache.druid.query.dimension.ColumnSelectorStrategy;
import org.apache.druid.segment.ColumnValueSelector;

/**
 * A grouping of various related objects used during query processing for a single dimension, used for convenience.
 *
 * Each ColumnSelectorPlus is associated with a single dimension.
 */
public class ColumnSelectorPlus<ColumnSelectorStrategyClass extends ColumnSelectorStrategy>
{
  /**
   * Helper object that handles row value operations that pertain to a specific query type for this
   * dimension within query processing engines.
   */
  private final ColumnSelectorStrategyClass columnSelectorStrategy;

  /**
   * Internal name of the dimension.
   */
  private final String name;

  /**
   * Name of the dimension to be returned in query results.
   */
  private final String outputName;

  /**
   * ColumnValueSelector for this dimension, e.g. a DimensionSelector for String dimensions.
   */
  private final ColumnValueSelector selector;

  public ColumnSelectorPlus(
      String columnName,
      String outputName,
      ColumnSelectorStrategyClass columnSelectorStrategy,
      ColumnValueSelector selector
  )
  {
    this.columnSelectorStrategy = columnSelectorStrategy;
    this.name = columnName;
    this.outputName = outputName;
    this.selector = selector;
  }

  public ColumnSelectorStrategyClass getColumnSelectorStrategy()
  {
    return columnSelectorStrategy;
  }

  public String getName()
  {
    return name;
  }

  public String getOutputName()
  {
    return outputName;
  }

  public ColumnValueSelector getSelector()
  {
    return selector;
  }
}
