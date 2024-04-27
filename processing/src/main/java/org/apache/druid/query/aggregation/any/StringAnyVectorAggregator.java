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

package org.apache.druid.query.aggregation.any;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class StringAnyVectorAggregator implements VectorAggregator
{
  private static final int FOUND_AND_NULL_FLAG_VALUE = -1;
  @VisibleForTesting
  static final int NOT_FOUND_FLAG_VALUE = -2;
  @VisibleForTesting
  static final int FOUND_VALUE_OFFSET = Integer.BYTES;

  @Nullable
  private final SingleValueDimensionVectorSelector singleValueSelector;
  @Nullable
  private final MultiValueDimensionVectorSelector multiValueSelector;
  private final int maxStringBytes;
  private final boolean aggregateMultipleValues;

  public StringAnyVectorAggregator(
      SingleValueDimensionVectorSelector singleValueSelector,
      MultiValueDimensionVectorSelector multiValueSelector,
      int maxStringBytes,
      final boolean aggregateMultipleValues
  )
  {
    Preconditions.checkState(
        singleValueSelector != null || multiValueSelector != null,
        "At least one selector must be non null"
    );
    Preconditions.checkState(
        singleValueSelector == null || multiValueSelector == null,
        "Only one selector must be non null"
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

  @Override
  public void aggregate(ByteBuffer buf, int position, int startRow, int endRow)
  {
    if (buf.getInt(position) == NOT_FOUND_FLAG_VALUE && startRow < endRow) {
      if (multiValueSelector != null) {
        final IndexedInts[] rows = multiValueSelector.getRowVector();
        if (startRow < rows.length) {
          IndexedInts row = rows[startRow];
          @Nullable
          String foundValue = readValue(row);
          putValue(buf, position, foundValue);
        }
      } else if (singleValueSelector != null) {
        final int[] rows = singleValueSelector.getRowVector();
        if (startRow < rows.length) {
          int row = rows[startRow];
          @Nullable
          String foundValue = singleValueSelector.lookupName(row);
          putValue(buf, position, foundValue);
        }
      }
    }
  }

  private String readValue(IndexedInts row)
  {
    if (row.size() == 0) {
      return null;
    }
    if (aggregateMultipleValues) {
      if (row.size() == 1) {
        return multiValueSelector.lookupName(row.get(0));
      }
      List<String> arrayList = new ArrayList<>();
      row.forEach(rowIndex -> {
        arrayList.add(multiValueSelector.lookupName(rowIndex));
      });
      return DimensionHandlerUtils.convertObjectToString(arrayList);
    }
    return multiValueSelector.lookupName(row.get(0));
  }

  @Override
  public void aggregate(
      ByteBuffer buf,
      int numRows,
      int[] positions, @Nullable int[] rows,
      int positionOffset
  )
  {
    for (int i = 0; i < numRows; i++) {
      int position = positions[i] + positionOffset;
      int row = rows == null ? i : rows[i];
      aggregate(buf, position, row, row + 1);
    }
  }

  @Nullable
  @Override
  public String get(ByteBuffer buf, int position)
  {
    ByteBuffer copyBuffer = buf.duplicate();
    copyBuffer.position(position);
    int stringSizeBytes = copyBuffer.getInt();
    if (stringSizeBytes >= 0) {
      byte[] valueBytes = new byte[stringSizeBytes];
      copyBuffer.get(valueBytes, 0, stringSizeBytes);
      return StringUtils.fromUtf8(valueBytes);
    } else {
      return null;
    }
  }

  @Override
  public void close()
  {
    // Nothing to close.
  }

  private void putValue(ByteBuffer buf, int position, @Nullable String foundValue)
  {
    if (foundValue != null) {
      ByteBuffer mutationBuffer = buf.duplicate();
      mutationBuffer.position(position + FOUND_VALUE_OFFSET);
      mutationBuffer.limit(position + FOUND_VALUE_OFFSET + maxStringBytes);
      final int len = StringUtils.toUtf8WithLimit(foundValue, mutationBuffer);
      mutationBuffer.putInt(position, len);
    } else {
      buf.putInt(position, FOUND_AND_NULL_FLAG_VALUE);
    }
  }

  public boolean isAggregateMultipleValues()
  {
    return aggregateMultipleValues;
  }
}
