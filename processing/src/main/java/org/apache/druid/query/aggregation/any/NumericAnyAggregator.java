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

package org.apache.druid.query.aggregation.any;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.BaseNullableColumnValueSelector;

/**
 * Base type for on heap 'any' aggregator for primitive numeric column selectors
 */
public abstract class NumericAnyAggregator<TSelector extends BaseNullableColumnValueSelector> implements Aggregator
{
  private final boolean useDefault = NullHandling.replaceWithDefault();
  final TSelector valueSelector;

  boolean isNull;
  boolean isFound;

  public NumericAnyAggregator(TSelector valueSelector)
  {
    this.valueSelector = valueSelector;
    this.isNull = !useDefault;
    this.isFound = false;
  }

  /**
   * Store the found primitive value
   */
  abstract void setFoundValue();

  @Override
  public void aggregate()
  {
    if (!isFound) {
      if (useDefault || !valueSelector.isNull()) {
        setFoundValue();
        isNull = false;
      } else {
        isNull = true;
      }
      isFound = true;
    }
  }

  @Override
  public void close()
  {
    // nothing to close
  }
}
