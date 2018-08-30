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

import org.apache.druid.query.ColumnSelectorPlus;
import org.apache.druid.query.topn.types.TopNColumnSelectorStrategy;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionSelector;

/**
 */
public class TopNParams
{
  private final Cursor cursor;
  private final int cardinality;
  private final int numValuesPerPass;
  private final ColumnSelectorPlus<TopNColumnSelectorStrategy> selectorPlus;

  protected TopNParams(
      ColumnSelectorPlus<TopNColumnSelectorStrategy> selectorPlus,
      Cursor cursor,
      int numValuesPerPass
  )
  {
    this.selectorPlus = selectorPlus;
    this.cursor = cursor;
    this.cardinality = selectorPlus.getColumnSelectorStrategy().getCardinality(selectorPlus.getSelector());
    this.numValuesPerPass = numValuesPerPass;
  }

  // Only used by TopN algorithms that support String exclusively
  // Otherwise, get an appropriately typed selector from getSelectorPlus()
  public DimensionSelector getDimSelector()
  {
    return (DimensionSelector) selectorPlus.getSelector();
  }

  public ColumnSelectorPlus<TopNColumnSelectorStrategy> getSelectorPlus()
  {
    return selectorPlus;
  }

  public Cursor getCursor()
  {
    return cursor;
  }

  public int getCardinality()
  {
    return cardinality;
  }

  public int getNumValuesPerPass()
  {
    return numValuesPerPass;
  }
}
