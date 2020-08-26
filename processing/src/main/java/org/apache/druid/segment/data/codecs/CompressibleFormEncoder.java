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
import org.apache.druid.segment.writeout.WriteOutBytes;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A CompressibleFormEncoder extends {@link FormEncoder} to allow composition with a {@link CompressedFormEncoder},
 * to further compress encoded values at the byte level in a value agnostic manner using any
 * {@link org.apache.druid.segment.data.CompressionStrategy}.
 *
 * @param <TChunk>
 * @param <TChunkMetrics>
 */
public interface CompressibleFormEncoder<TChunk, TChunkMetrics extends FormMetrics>
    extends FormEncoder<TChunk, TChunkMetrics>
{
  /**
   * Encode values to a temporary buffer to stage for compression by the
   * {@link org.apache.druid.segment.data.CompressionStrategy} in use by {@link CompressedFormEncoder}
   *
   * @param buffer
   * @param values
   * @param numValues
   * @param metadata
   *
   * @throws IOException
   */
  void encodeToBuffer(
      ByteBuffer buffer,
      TChunk values,
      int numValues,
      TChunkMetrics metadata
  ) throws IOException;

  /**
   * Encode any metadata, not including the encoding header byte, to {@link WriteOutBytes}, which will preceede
   * compressed values
   *
   * @param valuesOut
   * @param values
   * @param numValues
   * @param metrics
   *
   * @throws IOException
   */
  default void encodeCompressionMetadata(
      WriteOutBytes valuesOut,
      TChunk values,
      int numValues,
      TChunkMetrics metrics
  ) throws IOException
  {
  }

  /**
   * Get size of any encoding metadata, not including header byte.
   *
   * @return
   */
  default int getMetadataSize()
  {
    return 0;
  }

  /**
   * To be chill, implementations can suggest that
   * {@link ShapeShiftingColumnSerializer} should not attempt compression since it can be very
   * expensive in terms of compute time, skipping this encoding for consideration for a block of values.
   *
   * @param hints
   *
   * @return
   */
  default boolean shouldAttemptCompression(TChunkMetrics hints)
  {
    return true;
  }
}
