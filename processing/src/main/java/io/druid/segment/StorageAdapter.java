/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.segment;

import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.Indexed;
import org.joda.time.DateTime;
import org.joda.time.Interval;

/**
 */
public interface StorageAdapter extends CursorFactory
{
  public String getSegmentIdentifier();
  public Interval getInterval();
  public Indexed<String> getAvailableDimensions();
  public Iterable<String> getAvailableMetrics();

  /**
   * Returns the number of distinct values for the given dimension column
   * For dimensions of unknown cardinality, e.g. __time this currently returns
   * Integer.MAX_VALUE
   *
   * @param column
   * @return
   */
  public int getDimensionCardinality(String column);
  public DateTime getMinTime();
  public DateTime getMaxTime();
  public Capabilities getCapabilities();
  public ColumnCapabilities getColumnCapabilities(String column);
  public int getNumRows();
  public DateTime getMaxIngestedEventTime();
}
