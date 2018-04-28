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

package org.apache.druid.segment.data.codecs;

import org.apache.druid.segment.data.ShapeShiftingColumnSerializer;

import java.nio.ByteOrder;

/**
 * Common base type for {@link FormEncoder} implementations of any type of
 * {@link ShapeShiftingColumnSerializer}
 *
 * @param <TChunk>
 * @param <TChunkMetrics>
 */
public abstract class BaseFormEncoder<TChunk, TChunkMetrics extends FormMetrics>
    implements FormEncoder<TChunk, TChunkMetrics>
{
  protected final byte logValuesPerChunk;
  protected final int valuesPerChunk;
  protected final ByteOrder byteOrder;
  protected final boolean isBigEndian;

  public BaseFormEncoder(byte logValuesPerChunk, ByteOrder byteOrder)
  {
    this.logValuesPerChunk = logValuesPerChunk;
    this.valuesPerChunk = 1 << logValuesPerChunk;
    this.byteOrder = byteOrder;
    this.isBigEndian = byteOrder.equals(ByteOrder.BIG_ENDIAN);
  }
}
