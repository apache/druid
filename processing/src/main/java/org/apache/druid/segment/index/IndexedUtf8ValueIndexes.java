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

package org.apache.druid.segment.index;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.druid.annotations.SuppressFBWarnings;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.ByteBufferUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.TypeSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.index.semantic.StringValueSetIndexes;
import org.apache.druid.segment.index.semantic.Utf8ValueSetIndexes;
import org.apache.druid.segment.index.semantic.ValueIndexes;
import org.apache.druid.segment.index.semantic.ValueSetIndexes;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.SortedSet;

public final class IndexedUtf8ValueIndexes<TDictionary extends Indexed<ByteBuffer>>
    implements StringValueSetIndexes, Utf8ValueSetIndexes, ValueIndexes, ValueSetIndexes
{
  // This determines the cut-off point to switch the merging algorithm from doing binary-search per element in the value
  // set to doing a sorted merge algorithm between value set and dictionary. The ratio here represents the ratio b/w
  // the number of elements in value set and the number of elements in the dictionary. The number has been derived
  // using benchmark in https://github.com/apache/druid/pull/13133. If the ratio is higher than the threshold, we use
  // sorted merge instead of binary-search based algorithm.
  private static final double SORTED_MERGE_RATIO_THRESHOLD = 0.12D;
  private static final int SIZE_WORTH_CHECKING_MIN = 8;
  private static final Comparator<ByteBuffer> COMPARATOR = ByteBufferUtils.utf8Comparator();

  private final BitmapFactory bitmapFactory;
  private final TDictionary dictionary;
  private final Indexed<ImmutableBitmap> bitmaps;

  public IndexedUtf8ValueIndexes(
      BitmapFactory bitmapFactory,
      TDictionary dictionary,
      Indexed<ImmutableBitmap> bitmaps
  )
  {
    Preconditions.checkArgument(dictionary.isSorted(), "Dictionary must be sorted");
    this.bitmapFactory = bitmapFactory;
    this.dictionary = dictionary;
    this.bitmaps = bitmaps;
  }

  @Override
  public BitmapColumnIndex forValue(@Nullable String value)
  {
    final ByteBuffer utf8 = StringUtils.toUtf8ByteBuffer(value);
    return new SimpleBitmapColumnIndex()
    {

      @Override
      public <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory, boolean includeUnknown)
      {
        if (includeUnknown && NullHandling.isNullOrEquivalent(dictionary.get(0))) {
          return bitmapResultFactory.unionDimensionValueBitmaps(
              ImmutableList.of(getBitmapForValue(), getBitmap(0))
          );
        }
        return bitmapResultFactory.wrapDimensionValue(getBitmapForValue());
      }

      private ImmutableBitmap getBitmapForValue()
      {
        final int idx = dictionary.indexOf(utf8);
        return getBitmap(idx);
      }
    };
  }

  @Nullable
  @Override
  public BitmapColumnIndex forValue(@Nonnull Object value, TypeSignature<ValueType> valueType)
  {
    if (valueType.isPrimitive()) {
      return forValue(
          ExprEval.ofType(ExpressionType.fromColumnTypeStrict(valueType), value)
                  .castTo(ExpressionType.STRING)
                  .asString()
      );
    }
    return null;
  }

  @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION")
  @Override
  public BitmapColumnIndex forSortedValues(SortedSet<String> values)
  {
    return getBitmapColumnIndexForSortedIterableUtf8(
        Iterables.transform(
            values,
            StringUtils::toUtf8ByteBuffer
        ),
        values.size(),
        values.contains(null)
    );
  }

  @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION")
  @Override
  public BitmapColumnIndex forSortedValuesUtf8(List<ByteBuffer> sortedValuesUtf8)
  {
    if (sortedValuesUtf8.isEmpty()) {
      return new AllFalseBitmapColumnIndex(bitmapFactory);
    }
    final boolean matchNull = sortedValuesUtf8.get(0) == null;
    final List<ByteBuffer> tailSet;

    if (sortedValuesUtf8.size() >= SIZE_WORTH_CHECKING_MIN) {
      final ByteBuffer minValueInColumn = dictionary.get(0);
      final int position = Collections.binarySearch(
          sortedValuesUtf8,
          minValueInColumn,
          ByteBufferUtils.utf8Comparator()
      );
      tailSet = sortedValuesUtf8.subList(position >= 0 ? position : -(position + 1), sortedValuesUtf8.size());
    } else {
      tailSet = sortedValuesUtf8;
    }

    return getBitmapColumnIndexForSortedIterableUtf8(tailSet, tailSet.size(), matchNull);
  }

  private ImmutableBitmap getBitmap(int idx)
  {
    if (idx < 0) {
      return bitmapFactory.makeEmptyImmutableBitmap();
    }

    final ImmutableBitmap bitmap = bitmaps.get(idx);
    return bitmap == null ? bitmapFactory.makeEmptyImmutableBitmap() : bitmap;
  }

  /**
   * Helper used by {@link #forSortedValues} and {@link #forSortedValuesUtf8}.
   */
  private BitmapColumnIndex getBitmapColumnIndexForSortedIterableUtf8(
      Iterable<ByteBuffer> valuesUtf8,
      int size,
      boolean valuesContainsNull
  )
  {
    // for large number of in-filter values in comparison to the dictionary size, use the sorted merge algorithm.
    if (size > SORTED_MERGE_RATIO_THRESHOLD * dictionary.size()) {
      return ValueSetIndexes.buildBitmapColumnIndexFromSortedIteratorScan(
          bitmapFactory,
          COMPARATOR,
          valuesUtf8,
          dictionary,
          bitmaps,
          () -> {
            if (!valuesContainsNull && NullHandling.isNullOrEquivalent(dictionary.get(0))) {
              return bitmaps.get(0);
            }
            return null;
          }
      );
    }

    // if the size of in-filter values is less than the threshold percentage of dictionary size, then use binary search
    // based lookup per value. The algorithm works well for smaller number of values.
    return ValueSetIndexes.buildBitmapColumnIndexFromSortedIteratorBinarySearch(
        bitmapFactory,
        valuesUtf8,
        dictionary,
        bitmaps,
        () -> {
          if (!valuesContainsNull && NullHandling.isNullOrEquivalent(dictionary.get(0))) {
            return bitmaps.get(0);
          }
          return null;
        }
    );
  }

  @Nullable
  @Override
  public BitmapColumnIndex forSortedValues(@Nonnull List<?> sortedValues, TypeSignature<ValueType> matchValueType)
  {
    if (sortedValues.isEmpty()) {
      return new AllFalseBitmapColumnIndex(bitmapFactory);
    }
    final boolean matchNull = sortedValues.get(0) == null;
    final Supplier<ImmutableBitmap> unknownsIndex = () -> {
      if (!matchNull && dictionary.get(0) == null) {
        return bitmaps.get(0);
      }
      return null;
    };
    if (matchValueType.is(ValueType.STRING)) {
      final List<String> tailSet;
      final List<String> baseSet = (List<String>) sortedValues;

      if (sortedValues.size() >= ValueSetIndexes.SIZE_WORTH_CHECKING_MIN) {
        final Object minValueInColumn = dictionary.get(0);
        final int position = Collections.binarySearch(
            sortedValues,
            StringUtils.fromUtf8((ByteBuffer) minValueInColumn),
            matchValueType.getNullableStrategy()
        );
        tailSet = baseSet.subList(position >= 0 ? position : -(position + 1), baseSet.size());
      } else {
        tailSet = baseSet;
      }
      if (tailSet.size() > ValueSetIndexes.SORTED_SCAN_RATIO_THRESHOLD * dictionary.size()) {
        return ValueSetIndexes.buildBitmapColumnIndexFromSortedIteratorScan(
            bitmapFactory,
            ByteBufferUtils.utf8Comparator(),
            Iterables.transform(tailSet, StringUtils::toUtf8ByteBuffer),
            dictionary,
            bitmaps,
            unknownsIndex
        );
      }
      // fall through to value iteration
      return ValueSetIndexes.buildBitmapColumnIndexFromSortedIteratorBinarySearch(
          bitmapFactory,
          Iterables.transform(tailSet, StringUtils::toUtf8ByteBuffer),
          dictionary,
          bitmaps,
          unknownsIndex
      );
    } else {
      return ValueSetIndexes.buildBitmapColumnIndexFromIteratorBinarySearch(
          bitmapFactory,
          Iterables.transform(
              sortedValues,
              x -> StringUtils.toUtf8ByteBuffer(DimensionHandlerUtils.convertObjectToString(x))
          ),
          dictionary,
          bitmaps,
          unknownsIndex
      );
    }
  }
}
