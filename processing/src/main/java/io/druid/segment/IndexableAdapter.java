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

import io.druid.segment.column.BitmapIndexSeeker;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedInts;
import org.joda.time.Interval;

/**
 * An adapter to an index
 */
public interface IndexableAdapter
{
  Interval getDataInterval();

  int getNumRows();

  Indexed<String> getDimensionNames();

  Indexed<String> getMetricNames();

  Indexed<String> getDimValueLookup(String dimension);

  Iterable<Rowboat> getRows();

  IndexedInts getBitmapIndex(String dimension, String value);

  BitmapIndexSeeker getBitmapIndexSeeker(String dimension);

  String getMetricType(String metric);

  ColumnCapabilities getCapabilities(String column);
}
