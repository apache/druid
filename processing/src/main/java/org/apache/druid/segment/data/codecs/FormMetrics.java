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

import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.data.ShapeShiftingColumnSerializer;

/**
 * Base type for collecting statistics about a block of values for
 * {@link ShapeShiftingColumnSerializer} to provide to {@link FormEncoder} implementations to
 * make decisions about what encoding to employ.
 */
public abstract class FormMetrics
{
  private IndexSpec.ShapeShiftOptimizationTarget optimizationTarget;
  private boolean enableEncoderOptOut;

  private byte compressionBufferHolder = -1;

  public FormMetrics(IndexSpec.ShapeShiftOptimizationTarget optimizationTarget, boolean enableEncoderOptOut)
  {
    this.optimizationTarget = optimizationTarget;
    this.enableEncoderOptOut = enableEncoderOptOut;
  }

  /**
   * Get {@link IndexSpec.ShapeShiftOptimizationTarget}, useful for {@link FormEncoder}
   * implementations to adapt their calculations to the supplied indexing preference
   *
   * @return
   */
  public IndexSpec.ShapeShiftOptimizationTarget getOptimizationTarget()
  {
    return this.optimizationTarget;
  }

  /**
   * When multiple 'complete' encoders are being employed, allow encoders which 'think' they will perform poorly for a
   * given block to opt out of being used, which in some cases can save expensive calculations
   * @return
   */
  public boolean isEnableEncoderOptOut()
  {
    return enableEncoderOptOut;
  }

  /**
   * Total number of rows processed for this block of values
   *
   * @return
   */
  public abstract int getNumValues();

  /**
   * byte header value of last encoder to use compressed bytebuffer, allowing re-use if encoder is chosen rather than
   * recompressing
   *
   * @return
   */
  public byte getCompressionBufferHolder()
  {
    return compressionBufferHolder;
  }

  /**
   * Set encoder header as holder of compression buffers
   *
   * @param encoder
   */
  public void setCompressionBufferHolder(byte encoder)
  {
    this.compressionBufferHolder = encoder;
  }
}
