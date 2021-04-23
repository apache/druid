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

import org.apache.druid.collections.SerializablePair;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.ColumnValueSelector;

/**
 * Base type for on heap 'first' aggregator for primitive numeric column selectors
 */
public abstract class NumericFirstAggregator implements Aggregator
{
  private static final boolean USE_DEFAULT = NullHandling.replaceWithDefault();
  private final BaseLongColumnValueSelector timeSelector;
  private final ColumnValueSelector valueSelector;
  private final boolean needsFoldCheck;

  long firstTime;
  boolean rhsNull;

  public NumericFirstAggregator(
      BaseLongColumnValueSelector timeSelector,
      ColumnValueSelector valueSelector,
      boolean needsFoldCheck
  )
  {
    this.timeSelector = timeSelector;
    this.valueSelector = valueSelector;
    this.needsFoldCheck = needsFoldCheck;

    firstTime = Long.MAX_VALUE;
    rhsNull = !USE_DEFAULT;
  }

  /**
   * Store the current primitive typed 'first' value
   */
  abstract void setFirstValue(ColumnValueSelector valueSelector);

  /**
   * Store a non-null first value
   */
  abstract void setFirstValue(Number firstValue);

  @Override
  public void aggregate()
  {
    if (needsFoldCheck) {

      // Need to read this first (before time), just in case it's a SerializablePairLongString (we don't know; it's
      // detected at query time).
      final Object object = valueSelector.getObject();

      if (object instanceof SerializablePair) {

        // cast to Pair<Long, Number> to support reindex from type such as doubleFirst into longFirst
        final SerializablePair<Long, Number> pair = (SerializablePair<Long, Number>) object;
        if (pair.lhs < firstTime) {
          firstTime = pair.lhs;

          // rhs might be NULL under SQL-compatibility mode
          if (pair.rhs == null) {
            rhsNull = true;
          } else {
            rhsNull = false;
            setFirstValue(pair.rhs);
          }
        }

        return;
      }
    }

    long time = timeSelector.getLong();
    if (time < firstTime) {
      firstTime = time;
      if (USE_DEFAULT || !valueSelector.isNull()) {
        setFirstValue(valueSelector);
        rhsNull = false;
      } else {
        setFirstValue(0);
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
