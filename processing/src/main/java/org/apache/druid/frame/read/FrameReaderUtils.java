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

package org.apache.druid.frame.read;

import com.google.common.primitives.Ints;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.frame.allocation.MemoryRange;
import org.apache.druid.frame.segment.row.FrameColumnSelectorFactory;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.function.Supplier;

/**
 * Utility methods used by various entities that read data from {@link org.apache.druid.frame.Frame} objects.
 */
public class FrameReaderUtils
{
  /**
   * Returns a ByteBuffer containing data from the provided {@link Memory}. The ByteBuffer is always newly
   * created, so it is OK to change its position, limit, etc. However, it may point directly to the backing memory
   * of the {@link Memory} object, so it is not OK to write to its contents.
   */
  public static ByteBuffer readByteBuffer(final Memory memory, final long dataStart, final int dataLength)
  {
    if (memory.hasByteBuffer()) {
      // Avoid data copy
      final ByteBuffer byteBuffer = memory.getByteBuffer().duplicate();
      byteBuffer.limit(Ints.checkedCast(memory.getRegionOffset(dataStart + dataLength)));
      byteBuffer.position(Ints.checkedCast(memory.getRegionOffset(dataStart)));
      return byteBuffer;
    } else {
      final byte[] stringData = new byte[dataLength];
      memory.getByteArray(dataStart, stringData, 0, stringData.length);
      return ByteBuffer.wrap(stringData);
    }
  }

  /**
   * Returns a direct row memory supplier if {@link #mayBeAbleToSelectRowMemory}, otherwise returns null.
   *
   * Note that even if the returned supplier is nonnull, it is still possible for the supplied {@link MemoryRange}
   * to be null. Callers must check for this.
   *
   * @param columnSelectorFactory frame column selector factory
   * @param expectedSignature     expected signature of the target frame
   */
  @Nullable
  public static Supplier<MemoryRange<Memory>> makeRowMemorySupplier(
      final ColumnSelectorFactory columnSelectorFactory,
      final RowSignature expectedSignature
  )
  {
    if (mayBeAbleToSelectRowMemory(columnSelectorFactory)) {
      final ColumnValueSelector<?> signatureSelector =
          columnSelectorFactory.makeColumnValueSelector(FrameColumnSelectorFactory.ROW_SIGNATURE_COLUMN);

      final ColumnValueSelector<?> memorySelector =
          columnSelectorFactory.makeColumnValueSelector(FrameColumnSelectorFactory.ROW_MEMORY_COLUMN);

      return new Supplier<MemoryRange<Memory>>()
      {
        private RowSignature lastSignature = null;
        private boolean lastSignatureOk = false;

        @Override
        public MemoryRange<Memory> get()
        {
          final RowSignature selectedSignature = (RowSignature) signatureSelector.getObject();

          //noinspection ObjectEquality: checking reference equality on purpose
          if (selectedSignature != lastSignature) {
            lastSignature = selectedSignature;
            lastSignatureOk = expectedSignature.equals(selectedSignature);
          }

          if (lastSignatureOk) {
            //noinspection unchecked
            return (MemoryRange<Memory>) memorySelector.getObject();
          } else {
            return null;
          }
        }
      };
    } else {
      return null;
    }
  }

  /**
   * Compares two Memory ranges using unsigned byte ordering.
   *
   * Different from {@link Memory#compareTo}, which uses signed ordering.
   */
  public static int compareMemoryUnsigned(
      final Memory memory1,
      final long position1,
      final int length1,
      final Memory memory2,
      final long position2,
      final int length2
  )
  {
    final int commonLength = Math.min(length1, length2);

    for (int i = 0; i < commonLength; i += Long.BYTES) {
      final int remaining = commonLength - i;
      final long r1 = readComparableLong(memory1, position1 + i, remaining);
      final long r2 = readComparableLong(memory2, position2 + i, remaining);
      final int cmp = Long.compare(r1, r2);
      if (cmp != 0) {
        return cmp;
      }
    }

    return Integer.compare(length1, length2);
  }

  public static long readComparableLong(final Memory memory, final long position, final int length)
  {
    long retVal = 0;
    switch (length) {
      case 7:
        retVal |= (memory.getByte(position + 6) & 0xFFL) << 8;
      case 6:
        retVal |= (memory.getByte(position + 5) & 0xFFL) << 16;
      case 5:
        retVal |= (memory.getByte(position + 4) & 0xFFL) << 24;
      case 4:
        retVal |= (memory.getByte(position + 3) & 0xFFL) << 32;
      case 3:
        retVal |= (memory.getByte(position + 2) & 0xFFL) << 40;
      case 2:
        retVal |= (memory.getByte(position + 1) & 0xFFL) << 48;
      case 1:
        retVal |= (memory.getByte(position) & 0xFFL) << 56;
        break;
      default:
        retVal = Long.reverseBytes(memory.getLong(position));
    }

    return retVal + Long.MIN_VALUE;
  }

  /**
   * Compares Memory with a byte array using unsigned byte ordering.
   */
  public static int compareMemoryToByteArrayUnsigned(
      final Memory memory,
      final long position1,
      final long length1,
      final byte[] array,
      final int position2,
      final int length2
  )
  {
    final int commonLength = (int) Math.min(length1, length2);

    for (int i = 0; i < commonLength; i++) {
      final byte byte1 = memory.getByte(position1 + i);
      final byte byte2 = array[position2 + i];
      final int cmp = (byte1 & 0xFF) - (byte2 & 0xFF); // Unsigned comparison
      if (cmp != 0) {
        return cmp;
      }
    }

    return Long.compare(length1, length2);
  }

  /**
   * Compares two byte arrays using unsigned byte ordering.
   */
  public static int compareByteArraysUnsigned(
      final byte[] array1,
      final int position1,
      final int length1,
      final byte[] array2,
      final int position2,
      final int length2
  )
  {
    final int commonLength = Math.min(length1, length2);

    for (int i = 0; i < commonLength; i++) {
      final byte byte1 = array1[position1 + i];
      final byte byte2 = array2[position2 + i];
      final int cmp = (byte1 & 0xFF) - (byte2 & 0xFF); // Unsigned comparison
      if (cmp != 0) {
        return cmp;
      }
    }

    return Integer.compare(length1, length2);
  }

  /**
   * Returns whether a {@link ColumnSelectorFactory} may be able to provide a {@link MemoryRange}. This enables
   * efficient copying without needing to deal with each field individually.
   *
   * Note that if this method returns true, it may still not be possible to do direct row-memory copying.
   * Therefore, {@link #makeRowMemorySupplier} verifies the signature of each row.
   */
  private static boolean mayBeAbleToSelectRowMemory(final ColumnSelectorFactory columnSelectorFactory)
  {
    final ColumnCapabilities rowSignatureCapabilities =
        columnSelectorFactory.getColumnCapabilities(FrameColumnSelectorFactory.ROW_SIGNATURE_COLUMN);

    if (rowSignatureCapabilities == null || rowSignatureCapabilities.getType() != ValueType.COMPLEX) {
      return false;
    }

    final ColumnCapabilities rowMemoryCapabilities =
        columnSelectorFactory.getColumnCapabilities(FrameColumnSelectorFactory.ROW_MEMORY_COLUMN);

    if (rowMemoryCapabilities == null || rowMemoryCapabilities.getType() != ValueType.COMPLEX) {
      return false;
    }

    return true;
  }
}
