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

package io.druid.segment.column;

import io.druid.query.monomorphicprocessing.CalledFromHotLoop;
import io.druid.query.monomorphicprocessing.HotLoopCallee;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedFloats;
import io.druid.segment.data.IndexedLongs;

import java.io.Closeable;

/**
 */
public interface GenericColumn extends HotLoopCallee, Closeable
{
  public int length();
  public ValueType getType();
  public boolean hasMultipleValues();

  @CalledFromHotLoop
  public String getStringSingleValueRow(int rowNum);
  @CalledFromHotLoop
  public Indexed<String> getStringMultiValueRow(int rowNum);
  @CalledFromHotLoop
  public float getFloatSingleValueRow(int rowNum);
  @CalledFromHotLoop
  public IndexedFloats getFloatMultiValueRow(int rowNum);
  @CalledFromHotLoop
  public long getLongSingleValueRow(int rowNum);
  @CalledFromHotLoop
  public IndexedLongs getLongMultiValueRow(int rowNum);
  @CalledFromHotLoop
  double getDoubleSingleValueRow(int rowNum);

  @Override
  void close();
}
