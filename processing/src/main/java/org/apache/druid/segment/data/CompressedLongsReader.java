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

package org.apache.druid.segment.data;

import org.apache.druid.utils.CloseableUtils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Supplier;

public final class CompressedLongsReader implements ColumnarLongs
{
  public static Supplier<CompressedLongsReader> fromByteBuffer(ByteBuffer buffer, ByteOrder order)
  {
    final Supplier<CompressedBlockReader> baseReader = CompressedBlockReader.fromByteBuffer(buffer, order);
    return () -> new CompressedLongsReader(baseReader.get());
  }

  private final CompressedBlockReader blockReader;
  private final ByteBuffer buffer;

  public CompressedLongsReader(CompressedBlockReader blockReader)
  {
    this.blockReader = blockReader;
    this.buffer = blockReader.getDecompressedDataBuffer().asReadOnlyBuffer()
                             .order(blockReader.getDecompressedDataBuffer().order());
  }

  @Override
  public long get(int index)
  {
    final long byteOffset = (long) index * Long.BYTES;
    final int offset = blockReader.loadBlock(byteOffset);
    return buffer.getLong(offset);
  }

  @Override
  public int size()
  {
    return (int) (blockReader.getSize() / Long.BYTES);
  }

  @Override
  public void close()
  {
    CloseableUtils.closeAndWrapExceptions(blockReader);
  }
}
