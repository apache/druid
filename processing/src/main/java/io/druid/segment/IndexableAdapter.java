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

package io.druid.segment;

import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.BitmapValues;
import io.druid.segment.data.Indexed;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;

/**
 * An adapter to an index
 */
public interface IndexableAdapter
{
  Interval getDataInterval();

  int getNumRows();

  List<String> getDimensionNames();

  List<String> getMetricNames();

  @Nullable
  <T extends Comparable<T>> Indexed<T> getDimValueLookup(String dimension);

  TransformableRowIterator getRows();

  BitmapValues getBitmapValues(String dimension, int dictId);

  String getMetricType(String metric);

  ColumnCapabilities getCapabilities(String column);

  Metadata getMetadata();
}
