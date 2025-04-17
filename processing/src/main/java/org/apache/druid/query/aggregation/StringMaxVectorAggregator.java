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

package org.apache.druid.query.aggregation;

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class StringMaxVectorAggregator implements VectorAggregator
{
  private static final int FOUND_AND_NULL_FLAG_VALUE = -1;
  private static final int NOT_FOUND_FLAG_VALUE = -2;

  @Nullable
  private final SingleValueDimensionVectorSelector singleValueSelector;
  @Nullable
  private final MultiValueDimensionVectorSelector multiValueSelector;
  private final int maxStringBytes;
  private final boolean aggregateMultipleValues;

  public StringMaxVectorAggregator(
      @Nullable SingleValueDimensionVectorSelector singleValueSelector,
      @Nullable MultiValueDimensionVectorSelector multiValueSelector,
      int maxStringBytes,
      final boolean aggregateMultipleValues
  )
  {
    Preconditions.checkState(
        singleValueSelector == null ^ multiValueSelector == null,
        "Only one of [singleValueSelector] and [multiValueSelector] must be non null."
    );

    this.multiValueSelector = multiValueSelector;
    this.singleValueSelector = singleValueSelector;
    this.maxStringBytes = maxStringBytes;
    this.aggregateMultipleValues = aggregateMultipleValues;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putInt(position, NOT_FOUND_FLAG_VALUE);
  }

  @Nullable
  private String readValue(IndexedInts row)
  {
    Preconditions.checkNotNull(multiValueSelector);

    if (row.size() == 0) {
      return null;
    }
    if (aggregateMultipleValues) {
      if (row.size() == 1) {
        return multiValueSelector.lookupName(row.get(0));
      }
      List<String> arrayList = new ArrayList<>();
      row.forEach(rowIndex -> arrayList.add(multiValueSelector.lookupName(rowIndex)));
      return DimensionHandlerUtils.convertObjectToString(arrayList);
    }
    return multiValueSelector.lookupName(row.get(0));
  }

  @Override
  public void aggregate(ByteBuffer buf, int position, int startRow, int endRow)
  {
    if (startRow >= endRow) {
      return;
    }

    for (int i = startRow; i < endRow; i++) {
      if (multiValueSelector != null) {
        final IndexedInts[] rows = multiValueSelector.getRowVector();
        if (i < rows.length) {
          IndexedInts row = rows[i];
          String foundValue = readValue(row);
          updateMaxValue(buf, position, foundValue);
        }
      } else if (singleValueSelector != null) {
        final int[] rows = singleValueSelector.getRowVector();
        if (i < rows.length) {
          int row = rows[i];
          String foundValue = singleValueSelector.lookupName(row);
          updateMaxValue(buf, position, foundValue);
        }
      }
    }
  }

  @Override
  public void aggregate(
      ByteBuffer buf,
      int numRows,
      int[] positions,
      @Nullable int[] rows,
      int positionOffset
  )
  {
    for (int i = 0; i < numRows; i++) {
      int position = positions[i] + positionOffset;
      int row = rows == null ? i : rows[i];
      aggregate(buf, position, row, row + 1);
    }
  }

  private void updateMaxValue(ByteBuffer buf, int position, @Nullable String foundValue)
  {
    int currentFlag = buf.getInt(position);
    if (currentFlag == NOT_FOUND_FLAG_VALUE) {
      setMaxStringInBuffer(buf, position, foundValue);
      return;
    }

    String currentMaxValue = getMaxStringInBuffer(buf, position);
    if (foundValue != null && (currentMaxValue == null || foundValue.compareTo(currentMaxValue) > 0)) {
      setMaxStringInBuffer(buf, position, foundValue);
    }
  }

  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return getMaxStringInBuffer(buf, position);
  }

  @Override
  public void close()
  {
    // Nothing to close.
  }

  @Nullable
  private String getMaxStringInBuffer(ByteBuffer buf, int position)
  {
    int length = buf.getInt(position);
    if (length == FOUND_AND_NULL_FLAG_VALUE) {
      return null;
    }

    byte[] stringBytes = new byte[length];
    buf.position(position + Integer.BYTES);
    buf.get(stringBytes);
    buf.position(position); // Reset buffer position to avoid side effects.

    return StringUtils.fromUtf8(stringBytes);
  }

  private void setMaxStringInBuffer(ByteBuffer buf, int position, @Nullable String value)
  {
    if (value == null) {
      buf.putInt(position, FOUND_AND_NULL_FLAG_VALUE);
      return;
    }

    byte[] stringBytes = value.getBytes(StandardCharsets.UTF_8);
    int length = Math.min(stringBytes.length, maxStringBytes);

    buf.putInt(position, length);
    buf.position(position + Integer.BYTES);
    buf.put(stringBytes, 0, length);
    buf.position(position); // Reset buffer position to avoid side effects.
  }

  @Override
  public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
  {
    int length = oldBuffer.getInt(oldPosition);
    newBuffer.putInt(newPosition, length);

    if (length == FOUND_AND_NULL_FLAG_VALUE || length == NOT_FOUND_FLAG_VALUE) {
      return;
    }

    byte[] stringBytes = new byte[length];

    oldBuffer.position(oldPosition + Integer.BYTES);
    oldBuffer.get(stringBytes);

    newBuffer.position(newPosition + Integer.BYTES);
    newBuffer.put(stringBytes);

    // Reset buffer positions to avoid side effects
    oldBuffer.position(oldPosition);
    newBuffer.position(newPosition);
  }
}
