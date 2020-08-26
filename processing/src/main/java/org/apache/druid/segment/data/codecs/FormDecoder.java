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

/**
 * Interface describing value decoders for {@link ShapeShiftingColumn} implmentations. {@link ShapeShiftingColumn}
 * operate on principle of being mutated by {@link FormDecoder} to load a chunk of values, preparing it to be able to
 * read row values for indexes that fall within that chunk.
 *
 * @param <TColumn>
 */
public interface FormDecoder<TColumn extends ShapeShiftingColumn>
{
  /**
   * Transform {@link ShapeShiftingColumn} to be able to read values for this decoder.
   *
   * @param column
   */
  void transform(TColumn column);

  /**
   * Size of any chunk specific metadata stored at the start of a chunk, to calculate offset of values from chunk start
   * in underlying buffer
   *
   * @return
   */
  default int getMetadataSize()
  {
    return 0;
  }

  byte getHeader();
}
