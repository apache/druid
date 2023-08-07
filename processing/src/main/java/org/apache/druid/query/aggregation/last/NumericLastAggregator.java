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

package org.apache.druid.query.aggregation.last;

import org.apache.druid.collections.SerializablePair;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.ColumnValueSelector;

/**
 * Base type for on heap 'last' aggregator for primitive numeric column selectors..
 *
 * This could probably share a base class with {@link org.apache.druid.query.aggregation.first.NumericFirstAggregator}
 */
public abstract class NumericLastAggregator implements Aggregator
{
  private final boolean useDefault = NullHandling.replaceWithDefault();

  private final BaseLongColumnValueSelector timeSelector;
  final ColumnValueSelector valueSelector;
  final boolean needsFoldCheck;

  long lastTime;
  boolean rhsNull;

  public NumericLastAggregator(BaseLongColumnValueSelector timeSelector, ColumnValueSelector valueSelector, boolean needsFoldCheck)
  {
    this.timeSelector = timeSelector;
    this.valueSelector = valueSelector;
    this.needsFoldCheck = needsFoldCheck;

    lastTime = Long.MIN_VALUE;
    rhsNull = !useDefault;
  }

  /**
   * Store the current primitive typed 'last' value
   */
  abstract void setLastValue();

  abstract void setLastValue(Number lastValue);

  @Override
  public void aggregate()
  {
    if (timeSelector.isNull()) {
      return;
    }

    if (needsFoldCheck) {
      final Object object = valueSelector.getObject();
      if (object instanceof SerializablePair) {
        final SerializablePair<Long, Number> inPair = (SerializablePair<Long, Number>) object;

        if (inPair.lhs >= lastTime) {
          lastTime = inPair.lhs;

          if (inPair.rhs == null) {
            rhsNull = true;
          } else {
            rhsNull = false;
            setLastValue(inPair.rhs);
          }
        }
        return;
      }
    }
    long time = timeSelector.getLong();
    if (time >= lastTime) {
      lastTime = time;
      if (useDefault || !valueSelector.isNull()) {
        setLastValue();
        rhsNull = false;
      } else {
        setLastValue(0);
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
