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

package org.apache.druid.spectator.histogram;

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.data.CloseableIndexed;
import org.apache.druid.segment.data.IndexedIterable;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.Serializer;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;

/**
 * A generic, flat storage mechanism.  Use static SpectatorHistogramSerializer.create to construct.
 * Supports efficient storage for sparse columns that contain lots of nulls.
 * <p>
 * Storage Format:
 * <p>
 * byte 1: version (0x1)
 * byte 2: reserved flags
 * bytes 3-6 =>; numBytesUsed for header and values
 * bytes 7-some =>; header including count, bitmap of present values and offsets to values.
 * bytes (header.serializedSize + 6)-(numBytesUsed + 6): bytes representing the values. If offset is null, then the value is null.
 */
public class SpectatorHistogramIndexed implements CloseableIndexed<SpectatorHistogram>, Serializer
{
  static final byte VERSION_ONE = 0x1;
  static final byte RESERVED_FLAGS = 0x0;

  public static SpectatorHistogramIndexed read(ByteBuffer buffer, ObjectStrategy<SpectatorHistogram> strategy)
  {
    byte versionFromBuffer = buffer.get();

    if (VERSION_ONE == versionFromBuffer) {
      // Reserved flags, not currently used
      buffer.get();
      int sizeOfOffsetsAndValues = buffer.getInt();
      ByteBuffer bufferToUse = buffer.slice();
      bufferToUse.limit(sizeOfOffsetsAndValues);

      buffer.position(buffer.position() + sizeOfOffsetsAndValues);

      return new SpectatorHistogramIndexed(
          bufferToUse,
          strategy
      );
    }
    throw new IAE("Unknown version[%d]", (int) versionFromBuffer);
  }

  private final ObjectStrategy<SpectatorHistogram> strategy;
  private final int size;
  private final NullableOffsetsHeader offsetsHeader;
  private final ByteBuffer valueBuffer;

  private SpectatorHistogramIndexed(
      ByteBuffer buffer,
      ObjectStrategy<SpectatorHistogram> strategy
  )
  {
    this.strategy = strategy;
    offsetsHeader = NullableOffsetsHeader.read(buffer);
    // Size is count of entries
    size = offsetsHeader.size();
    // The rest of the buffer is the values
    valueBuffer = buffer.slice();
  }

  /**
   * Checks  if {@code index} a valid `element index` in SpectatorHistogramIndexed.
   * Similar to Preconditions.checkElementIndex() except this method throws {@link IAE} with custom error message.
   * <p>
   * Used here to get existing behavior(same error message and exception) of V1 GenericIndexed.
   *
   * @param index index identifying an element of an SpectatorHistogramIndexed.
   */
  private void checkIndex(int index)
  {
    if (index < 0) {
      throw new IAE("Index[%s] < 0", index);
    }
    if (index >= size) {
      throw new IAE("Index[%d] >= size[%d]", index, size);
    }
  }

  public Class<? extends SpectatorHistogram> getClazz()
  {
    return strategy.getClazz();
  }

  @Override
  public int size()
  {
    return size;
  }

  @Nullable
  @Override
  public SpectatorHistogram get(int index)
  {
    checkIndex(index);

    NullableOffsetsHeader.Offset offset = offsetsHeader.get(index);
    if (offset == null) {
      return null;
    }

    ByteBuffer copyValueBuffer = valueBuffer.asReadOnlyBuffer();
    copyValueBuffer.position(offset.getStart());
    copyValueBuffer.limit(offset.getStart() + offset.getLength());

    return strategy.fromByteBuffer(copyValueBuffer, offset.getLength());
  }

  @Override
  public int indexOf(@Nullable SpectatorHistogram value)
  {
    throw new UnsupportedOperationException("Reverse lookup not allowed.");
  }

  @Override
  public Iterator<SpectatorHistogram> iterator()
  {
    return IndexedIterable.create(this).iterator();
  }

  @Override
  public long getSerializedSize()
  {
    throw new UnsupportedOperationException("Serialization not supported here");
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher)
  {
    throw new UnsupportedOperationException("Serialization not supported here");
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("headerBuffer", offsetsHeader);
    inspector.visit("firstValueBuffer", valueBuffer);
    inspector.visit("strategy", strategy);
  }

  @Override
  public String toString()
  {
    return "SpectatorHistogramIndexed[" + "size: "
           + size()
           + " cardinality: "
           + offsetsHeader.getCardinality()
           + ']';
  }

  @Override
  public void close()
  {
    // nothing to close
  }
}
