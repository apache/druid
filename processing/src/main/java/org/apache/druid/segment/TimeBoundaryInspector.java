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

package org.apache.druid.segment;

import org.apache.druid.segment.column.ColumnHolder;
import org.joda.time.DateTime;
import org.joda.time.Interval;

/**
 * Returns min/max values of {@link ColumnHolder#TIME_COLUMN_NAME} for a segment. Typically retrieved from
 * {@link Segment#as(Class)}.
 *
 * Only offered by segments that are able to provide more detailed min/max values via metadata inspection than are
 * available from {@link Segment#getDataInterval()}.
 */
public interface TimeBoundaryInspector
{
  /**
   * Lower bound on {@link ColumnHolder#TIME_COLUMN_NAME}. Matches the lowest timestamp in the dataset
   * if {@link #isMinMaxExact()}.
   */
  DateTime getMinTime();

  /**
   * Upper bound on {@link ColumnHolder#TIME_COLUMN_NAME}. Strict if {@link #isMinMaxExact()}. Matches the highest
   * timestamp in the dataset if {@link #isMinMaxExact()}.
   */
  DateTime getMaxTime();

  /**
   * Smallest interval that contains {@link #getMinTime()} and {@link #getMaxTime()}. The endpoint is one millisecond
   * higher than {@link #getMaxTime()}.
   */
  default Interval getMinMaxInterval()
  {
    return new Interval(getMinTime(), getMaxTime().plus(1));
  }

  /**
   * Whether the lower and upper bounds returned by {@link #getMinTime()} and {@link #getMaxTime()} are actually
   * found in the dataset. If true, the bounds are strict and can be used substitutes for aggregations
   * {@code MIN(__time)} and {@code MAX(__time)}.
   */
  boolean isMinMaxExact();
}
