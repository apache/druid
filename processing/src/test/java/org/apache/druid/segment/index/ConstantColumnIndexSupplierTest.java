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

import com.google.common.collect.ImmutableList;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.DefaultBitmapResultFactory;
import org.apache.druid.query.filter.SelectorPredicateFactory;
import org.apache.druid.segment.column.BaseColumnHolder;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ConstantColumns;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.druid.segment.index.semantic.DruidPredicateIndexes;
import org.apache.druid.segment.index.semantic.NullValueIndex;
import org.apache.druid.segment.index.semantic.ValueIndexes;
import org.apache.druid.segment.index.semantic.ValueSetIndexes;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.util.Collections;

public class ConstantColumnIndexSupplierTest extends InitializedNullHandlingTest
{
  private static final int NUM_ROWS = 100;
  private static final BitmapFactory BITMAP_FACTORY = RoaringBitmapSerdeFactory.getInstance().getBitmapFactory();
  private static final BitmapResultFactory<ImmutableBitmap> BITMAP_RESULT_FACTORY =
      new DefaultBitmapResultFactory(BITMAP_FACTORY);

  @Test
  public void testUnsupportedType()
  {
    Assertions.assertThrows(
        DruidException.class,
        () -> makeSupplier(ColumnType.STRING_ARRAY, null)
    );
  }

  @Test
  public void testNullValueIndex()
  {
    assertIsTrue(makeSupplier(ColumnType.STRING, null).as(NullValueIndex.class).get());
    assertIsTrue(makeSupplier(ColumnType.LONG, null).as(NullValueIndex.class).get());
    assertIsTrue(makeSupplier(ColumnType.LONG, "abc").as(NullValueIndex.class).get()); // true due to coercion
    assertIsFalse(makeSupplier(ColumnType.STRING, "abc").as(NullValueIndex.class).get());
    assertIsFalse(makeSupplier(ColumnType.LONG, 10L).as(NullValueIndex.class).get());
  }

  @Test
  public void testValueIndexesString()
  {
    final ValueIndexes indexes = makeSupplier(ColumnType.STRING, "10").as(ValueIndexes.class);

    assertIsTrue(indexes.forValue("10", ColumnType.STRING));
    assertIsFalse(indexes.forValue("abc", ColumnType.STRING));
    assertIsTrue(indexes.forValue(10L, ColumnType.LONG));
    assertIsFalse(indexes.forValue(11L, ColumnType.LONG));
    assertIsFalse(indexes.forValue(10.0, ColumnType.DOUBLE)); // Because it stringifies to 10.0
    Assertions.assertNull(indexes.forValue(new Object[]{"10"}, ColumnType.STRING_ARRAY));
  }

  @Test
  public void testValueIndexesLong()
  {
    final ValueIndexes indexes = makeSupplier(ColumnType.LONG, 10L).as(ValueIndexes.class);

    assertIsTrue(indexes.forValue(10L, ColumnType.LONG));
    assertIsFalse(indexes.forValue(11L, ColumnType.LONG));
    assertIsTrue(indexes.forValue("10", ColumnType.STRING));
    assertIsTrue(indexes.forValue("10.0", ColumnType.STRING));
    assertIsFalse(indexes.forValue("abc", ColumnType.STRING));
    assertIsTrue(indexes.forValue(10.0, ColumnType.DOUBLE));
    assertIsFalse(indexes.forValue(10.5, ColumnType.DOUBLE));
  }

  @Test
  public void testValueIndexesFloat()
  {
    final ValueIndexes indexes = makeSupplier(ColumnType.FLOAT, 1.1f).as(ValueIndexes.class);

    assertIsTrue(indexes.forValue(1.1f, ColumnType.FLOAT));
    assertIsTrue(indexes.forValue(1.1, ColumnType.DOUBLE));
    assertIsFalse(indexes.forValue(1.2, ColumnType.DOUBLE));
    assertIsFalse(indexes.forValue(1L, ColumnType.LONG));
  }

  @Test
  public void testValueIndexesDouble()
  {
    final ValueIndexes indexes = makeSupplier(ColumnType.DOUBLE, 1.5).as(ValueIndexes.class);

    assertIsTrue(indexes.forValue(1.5, ColumnType.DOUBLE));
    assertIsFalse(indexes.forValue(1.6, ColumnType.DOUBLE));
    assertIsTrue(indexes.forValue("1.5", ColumnType.STRING));
    assertIsFalse(indexes.forValue(1L, ColumnType.LONG));
  }

  @Test
  public void testValueIndexesNullConstant()
  {
    assertIsUnknown(makeSupplier(ColumnType.STRING, null).as(ValueIndexes.class).forValue("abc", ColumnType.STRING));
    assertIsUnknown(makeSupplier(ColumnType.LONG, null).as(ValueIndexes.class).forValue(10L, ColumnType.LONG));
  }

  @Test
  public void testValueSetIndexesString()
  {
    final ValueSetIndexes indexes = makeSupplier(ColumnType.STRING, "10").as(ValueSetIndexes.class);

    assertIsTrue(indexes.forSortedValues(ImmutableList.of("10", "abc"), ColumnType.STRING));
    assertIsFalse(indexes.forSortedValues(ImmutableList.of("11", "abc"), ColumnType.STRING));
    assertIsTrue(indexes.forSortedValues(ImmutableList.of(1L, 10L), ColumnType.LONG));
    assertIsFalse(indexes.forSortedValues(ImmutableList.of(1L, 11L), ColumnType.LONG));
    assertIsFalse(indexes.forSortedValues(ImmutableList.of(10.0), ColumnType.DOUBLE)); // Because it stringifies to 10.0
  }

  @Test
  public void testValueSetIndexesLong()
  {
    final ValueSetIndexes indexes = makeSupplier(ColumnType.LONG, 10L).as(ValueSetIndexes.class);

    assertIsTrue(indexes.forSortedValues(ImmutableList.of(1L, 10L, 100L), ColumnType.LONG));
    assertIsFalse(indexes.forSortedValues(ImmutableList.of(1L, 100L), ColumnType.LONG));
    assertIsTrue(indexes.forSortedValues(ImmutableList.of("1", "10"), ColumnType.STRING));
    assertIsTrue(indexes.forSortedValues(ImmutableList.of("1", "10.0"), ColumnType.STRING));
    assertIsFalse(indexes.forSortedValues(ImmutableList.of("1", "abc"), ColumnType.STRING));
    assertIsTrue(indexes.forSortedValues(ImmutableList.of(9.5, 10.0), ColumnType.DOUBLE));
    assertIsFalse(indexes.forSortedValues(ImmutableList.of(9.5, 10.5), ColumnType.DOUBLE));
    assertIsFalse(indexes.forSortedValues(Collections.emptyList(), ColumnType.LONG));
  }

  @Test
  public void testValueSetIndexesFloat()
  {
    final ValueSetIndexes indexes = makeSupplier(ColumnType.FLOAT, 1.1f).as(ValueSetIndexes.class);

    assertIsTrue(indexes.forSortedValues(ImmutableList.of(1.0f, 1.1f), ColumnType.FLOAT));
    assertIsFalse(indexes.forSortedValues(ImmutableList.of(1.0f, 1.2f), ColumnType.FLOAT));
    assertIsTrue(indexes.forSortedValues(ImmutableList.of(1.1), ColumnType.DOUBLE));
    assertIsFalse(indexes.forSortedValues(ImmutableList.of(1.2), ColumnType.DOUBLE));
    assertIsFalse(indexes.forSortedValues(ImmutableList.of(1L), ColumnType.LONG));
  }

  @Test
  public void testValueSetIndexesDouble()
  {
    final ValueSetIndexes indexes = makeSupplier(ColumnType.DOUBLE, 1.5).as(ValueSetIndexes.class);

    assertIsTrue(indexes.forSortedValues(ImmutableList.of(1.0, 1.5), ColumnType.DOUBLE));
    assertIsFalse(indexes.forSortedValues(ImmutableList.of(1.0, 1.6), ColumnType.DOUBLE));
    assertIsTrue(indexes.forSortedValues(ImmutableList.of("1.5"), ColumnType.STRING));
    assertIsFalse(indexes.forSortedValues(ImmutableList.of(1L), ColumnType.LONG));
  }

  @Test
  public void testValueSetIndexesNullConstant()
  {
    final ValueSetIndexes indexes = makeSupplier(ColumnType.LONG, null).as(ValueSetIndexes.class);

    assertIsUnknown(indexes.forSortedValues(ImmutableList.of(1L, 10L), ColumnType.LONG));
    assertIsFalse(indexes.forSortedValues(Collections.emptyList(), ColumnType.LONG));
    // null in the value set matches null rows, matching the behavior of dictionary backed value set indexes
    assertIsTrue(indexes.forSortedValues(Collections.singletonList(null), ColumnType.LONG));
  }

  @Test
  public void testValueSetIndexesNonPrimitiveMatchType()
  {
    final ValueSetIndexes indexes = makeSupplier(ColumnType.LONG, 10L).as(ValueSetIndexes.class);
    Assertions.assertNull(indexes.forSortedValues(ImmutableList.of(new Object[]{10L}), ColumnType.LONG_ARRAY));
  }

  @Test
  public void testPredicateIndexesString()
  {
    final DruidPredicateIndexes indexes = makeSupplier(ColumnType.STRING, "abc").as(DruidPredicateIndexes.class);

    assertIsTrue(indexes.forPredicate(new SelectorPredicateFactory("abc")));
    assertIsFalse(indexes.forPredicate(new SelectorPredicateFactory("def")));
    assertIsFalse(indexes.forPredicate(new SelectorPredicateFactory(null)));
  }

  @Test
  public void testPredicateIndexesLong()
  {
    final DruidPredicateIndexes indexes = makeSupplier(ColumnType.LONG, 10L).as(DruidPredicateIndexes.class);

    assertIsTrue(indexes.forPredicate(new SelectorPredicateFactory("10")));
    assertIsFalse(indexes.forPredicate(new SelectorPredicateFactory("11")));
    assertIsFalse(indexes.forPredicate(new SelectorPredicateFactory("abc")));
    assertIsFalse(indexes.forPredicate(new SelectorPredicateFactory(null)));
  }

  @Test
  public void testPredicateIndexesFloat()
  {
    final DruidPredicateIndexes indexes = makeSupplier(ColumnType.FLOAT, 1.1f).as(DruidPredicateIndexes.class);

    assertIsTrue(indexes.forPredicate(new SelectorPredicateFactory("1.1")));
    assertIsFalse(indexes.forPredicate(new SelectorPredicateFactory("1.2")));
    assertIsFalse(indexes.forPredicate(new SelectorPredicateFactory(null)));
  }

  @Test
  public void testPredicateIndexesDouble()
  {
    final DruidPredicateIndexes indexes = makeSupplier(ColumnType.DOUBLE, 1.5).as(DruidPredicateIndexes.class);

    assertIsTrue(indexes.forPredicate(new SelectorPredicateFactory("1.5")));
    assertIsFalse(indexes.forPredicate(new SelectorPredicateFactory("1.6")));
    assertIsFalse(indexes.forPredicate(new SelectorPredicateFactory(null)));
  }

  @Test
  public void testPredicateIndexesNullConstant()
  {
    assertIsUnknown(
        makeSupplier(ColumnType.LONG, null).as(DruidPredicateIndexes.class)
                                           .forPredicate(new SelectorPredicateFactory("10"))
    );
    assertIsTrue(
        makeSupplier(ColumnType.LONG, null).as(DruidPredicateIndexes.class)
                                           .forPredicate(new SelectorPredicateFactory(null))
    );
    assertIsUnknown(
        makeSupplier(ColumnType.STRING, null).as(DruidPredicateIndexes.class)
                                             .forPredicate(new SelectorPredicateFactory("abc"))
    );
    assertIsTrue(
        makeSupplier(ColumnType.STRING, null).as(DruidPredicateIndexes.class)
                                             .forPredicate(new SelectorPredicateFactory(null))
    );
  }

  @Test
  public void testUnsupportedIndexes()
  {
    Assertions.assertNull(makeSupplier(ColumnType.STRING, "abc").as(String.class));
  }

  @Test
  public void testConstantColumnHolderSuppliesIndexes()
  {
    final BaseColumnHolder holder =
        ConstantColumns.makeConstantColumnHolder(ColumnType.STRING, "abc", NUM_ROWS, BITMAP_FACTORY);
    final ColumnIndexSupplier indexSupplier = holder.getIndexSupplier();
    Assertions.assertInstanceOf(ConstantColumnIndexSupplier.class, indexSupplier);
    assertIsTrue(indexSupplier.as(ValueIndexes.class).forValue("abc", ColumnType.STRING));
    assertIsFalse(indexSupplier.as(NullValueIndex.class).get());
  }

  private static ConstantColumnIndexSupplier makeSupplier(ColumnType type, @Nullable Object value)
  {
    return new ConstantColumnIndexSupplier(type, value, NUM_ROWS, BITMAP_FACTORY);
  }

  private static void assertIsTrue(@Nullable BitmapColumnIndex index)
  {
    Assertions.assertNotNull(index);
    Assertions.assertEquals(NUM_ROWS, cardinality(index, false));
    Assertions.assertEquals(NUM_ROWS, cardinality(index, true));
  }

  private static void assertIsFalse(@Nullable BitmapColumnIndex index)
  {
    Assertions.assertNotNull(index);
    Assertions.assertEquals(0, cardinality(index, false));
    Assertions.assertEquals(0, cardinality(index, true));
  }

  private static void assertIsUnknown(@Nullable BitmapColumnIndex index)
  {
    Assertions.assertNotNull(index);
    Assertions.assertEquals(0, cardinality(index, false));
    Assertions.assertEquals(NUM_ROWS, cardinality(index, true));
  }

  private static int cardinality(BitmapColumnIndex index, boolean includeUnknown)
  {
    return index.computeBitmapResult(BITMAP_RESULT_FACTORY, includeUnknown).size();
  }
}
