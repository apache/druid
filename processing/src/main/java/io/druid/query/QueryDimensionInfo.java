/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query;

import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.ColumnValueSelector;
import io.druid.segment.DimensionQueryHelper;

/**
 * A grouping of various related objects used during query processing for a single dimension, used for convenience.
 *
 * Each QueryDimensionInfo is associated with a single dimension.
 */
public class QueryDimensionInfo
{
  /**
   * The DimensionSpec representing this QueryDimensionInfo's dimension, taken from the query being processed.
   */
  public final DimensionSpec spec;

  /**
   * Helper object that handles type-specific operations for this dimension within query processing engines.
   */
  public final DimensionQueryHelper queryHelper;

  /**
   * Internal name of the dimension.
   */
  public final String name;

  /**
   * Name of the dimension to be returned in query results.
   */
  public final String outputName;

  /**
   * Column value selector for this dimension, e.g. a DimensionSelector for String dimensions.
   */
  public final ColumnValueSelector selector;

  /**
   * Cardinality of the dimension's value set, taken from the queryHelper.
   */
  public final int cardinality;

  /**
   * Used by the GroupBy engines, indicates the offset of this dimension's value within the grouping key.
   */
  public final int keyBufferPosition;


  public QueryDimensionInfo(
      DimensionSpec spec,
      DimensionQueryHelper queryHelper,
      ColumnValueSelector selector,
      int keyBufferPosition
  )
  {
    this.spec = spec;
    this.queryHelper = queryHelper;
    this.name = spec.getDimension();
    this.outputName = spec.getOutputName();
    this.selector = selector;
    this.cardinality = queryHelper.getCardinality(selector);
    this.keyBufferPosition = keyBufferPosition;
  }
}
