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

package org.apache.druid.data.input;

import org.apache.druid.utils.CompressionUtils;

import javax.annotation.Nullable;

/**
 * A class storing some attributes of an input file.
 * This information is used to make splits in the parallel indexing.
 *
 * @see SplitHintSpec
 * @see org.apache.druid.data.input.impl.SplittableInputSource
 */
public class InputFileAttribute
{
  /**
   * The size of the input file.
   */
  private final long size;

  /**
   * The path of the input file.
   */
  @Nullable
  private final CompressionUtils.Format compressionFormat;

  public InputFileAttribute(long size)
  {
    this(size, null);
  }

  public InputFileAttribute(long size, @Nullable CompressionUtils.Format compressionFormat)
  {
    this.size = size;
    this.compressionFormat = compressionFormat;
  }

  @Nullable
  public CompressionUtils.Format getCompressionFormat()
  {
    return compressionFormat;
  }

  public long getSize()
  {
    return size;
  }
}
