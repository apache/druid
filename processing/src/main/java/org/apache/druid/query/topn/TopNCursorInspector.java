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

package org.apache.druid.query.topn;

import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.TopNOptimizationInspector;
import org.joda.time.Interval;

import javax.annotation.Nullable;

public class TopNCursorInspector
{
  private final ColumnInspector columnInspector;
  @Nullable
  private final TopNOptimizationInspector topNOptimizationInspector;
  private final Interval dataInterval;

  private final int dimensionCardinality;

  public TopNCursorInspector(
      ColumnInspector columnInspector,
      @Nullable TopNOptimizationInspector topNOptimizationInspector,
      Interval dataInterval,
      int dimensionCardinality
  )
  {
    this.columnInspector = columnInspector;
    this.topNOptimizationInspector = topNOptimizationInspector;
    this.dataInterval = dataInterval;
    this.dimensionCardinality = dimensionCardinality;
  }

  public ColumnInspector getColumnInspector()
  {
    return columnInspector;
  }

  @Nullable
  public TopNOptimizationInspector getOptimizationInspector()
  {
    return topNOptimizationInspector;
  }

  public Interval getDataInterval()
  {
    return dataInterval;
  }

  public int getDimensionCardinality()
  {
    return dimensionCardinality;
  }
}
