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

package io.druid.query.filter;

import io.druid.collections.bitmap.BitmapFactory;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.collections.spatial.ImmutableRTree;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.data.Indexed;

/**
 */
public interface BitmapIndexSelector
{
  Indexed<String> getDimensionValues(String dimension);
  boolean hasMultipleValues(String dimension);
  int getNumRows();
  BitmapFactory getBitmapFactory();
  BitmapIndex getBitmapIndex(String dimension);
  ImmutableBitmap getBitmapIndex(String dimension, String value);
  ImmutableRTree getSpatialIndex(String dimension);
}
