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

import org.apache.druid.segment.data.ShapeShiftingColumn;
import org.apache.druid.segment.data.ShapeShiftingColumnSerializer;
import org.apache.druid.segment.writeout.WriteOutBytes;

import java.io.IOException;

/**
 * Interface describing value encoders for use with {@link ShapeShiftingColumnSerializer}
 *
 * @param <TChunk>        Type of value chunk, i.e. {@code int[]}, {@code long[]}, etc.
 * @param <TChunkMetrics> Type of {@link FormMetrics} that the encoder cosumes
 */
public interface FormEncoder<TChunk, TChunkMetrics extends FormMetrics>
{
  /**
   * Get size in bytes if the values were encoded with this encoder
   *
   * @param values
   * @param numValues
   * @param metrics
   *
   * @return
   *
   * @throws IOException
   */
  int getEncodedSize(
      TChunk values,
      int numValues,
      TChunkMetrics metrics
  ) throws IOException;

  default double getSpeedModifier(TChunkMetrics metrics)
  {
    return 1.0;
  }

  default double getModifiedEncodedSize(
      TChunk values,
      int numValues,
      TChunkMetrics metrics
  ) throws IOException
  {
    return getSpeedModifier(metrics) * getEncodedSize(values, numValues, metrics);
  }

  /**
   * Encode the values to the supplied {@link WriteOutBytes}
   *
   * @param valuesOut
   * @param values
   * @param numValues
   * @param metrics
   *
   * @throws IOException
   */
  void encode(
      WriteOutBytes valuesOut,
      TChunk values,
      int numValues,
      TChunkMetrics metrics
  ) throws IOException;

  /**
   * Byte value to write as first byte to indicate the type of encoder used for this chunk. This value must be distinct
   * for all encoding/decoding strategies tied to a specific implementation of
   * {@link ShapeShiftingColumnSerializer} and {@link ShapeShiftingColumn}
   *
   * @return
   */
  byte getHeader();

  /**
   * Get friendly name of this encoder
   *
   * @return
   */
  String getName();
}
