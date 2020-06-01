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

package org.apache.druid.query.aggregation.first;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.BaseNullableColumnValueSelector;

/**
 * Base type for on heap 'first' aggregator for primitive numeric column selectors
 */
public abstract class NumericFirstAggregator<TSelector extends BaseNullableColumnValueSelector> implements Aggregator
{
  private final boolean useDefault = NullHandling.replaceWithDefault();
  private final BaseLongColumnValueSelector timeSelector;

  final TSelector valueSelector;

  long firstTime;
  boolean rhsNull;

  public NumericFirstAggregator(BaseLongColumnValueSelector timeSelector, TSelector valueSelector)
  {
    this.timeSelector = timeSelector;
    this.valueSelector = valueSelector;

    firstTime = Long.MAX_VALUE;
    rhsNull = !useDefault;
  }

  /**
   * Store the current primitive typed 'first' value
   */
  abstract void setCurrentValue();

  @Override
  public void aggregate()
  {
    long time = timeSelector.getLong();
    if (time < firstTime) {
      firstTime = time;
      if (useDefault || !valueSelector.isNull()) {
        setCurrentValue();
        rhsNull = false;
      } else {
        rhsNull = true;
      }
    }
  }

  @Override
  public void close()
  {
    // nothing to close
  }
}
