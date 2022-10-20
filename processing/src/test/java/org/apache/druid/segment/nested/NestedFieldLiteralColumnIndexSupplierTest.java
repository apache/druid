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

package org.apache.druid.segment.nested;

import com.google.common.collect.ImmutableSet;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.DefaultBitmapResultFactory;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.segment.column.BitmapColumnIndex;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.DictionaryEncodedStringValueIndex;
import org.apache.druid.segment.column.DictionaryEncodedValueIndex;
import org.apache.druid.segment.column.DruidPredicateIndex;
import org.apache.druid.segment.column.LexicographicalRangeIndex;
import org.apache.druid.segment.column.NullValueIndex;
import org.apache.druid.segment.column.NumericRangeIndex;
import org.apache.druid.segment.column.StringValueSetIndex;
import org.apache.druid.segment.column.TypeStrategies;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.FixedIndexed;
import org.apache.druid.segment.data.FixedIndexedWriter;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.GenericIndexedWriter;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.druid.segment.serde.Serializer;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMedium;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.roaringbitmap.IntIterator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.TreeSet;

public class NestedFieldLiteralColumnIndexSupplierTest extends InitializedNullHandlingTest
{
  BitmapSerdeFactory roaringFactory = new RoaringBitmapSerdeFactory(null);
  BitmapResultFactory<ImmutableBitmap> bitmapResultFactory = new DefaultBitmapResultFactory(
      roaringFactory.getBitmapFactory()
  );
  GenericIndexed<String> globalStrings;
  FixedIndexed<Long> globalLongs;
  FixedIndexed<Double> globalDoubles;

  @Before
  public void setup() throws IOException
  {
    ByteBuffer stringBuffer = ByteBuffer.allocate(1 << 12);
    ByteBuffer longBuffer = ByteBuffer.allocate(1 << 12).order(ByteOrder.nativeOrder());
    ByteBuffer doubleBuffer = ByteBuffer.allocate(1 << 12).order(ByteOrder.nativeOrder());

    GenericIndexedWriter<String> stringWriter = new GenericIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        "strings",
        GenericIndexed.STRING_STRATEGY
    );
    stringWriter.open();
    stringWriter.write(null);
    stringWriter.write("a");
    stringWriter.write("b");
    stringWriter.write("fo");
    stringWriter.write("foo");
    stringWriter.write("fooo");
    stringWriter.write("z");
    writeToBuffer(stringBuffer, stringWriter);

    FixedIndexedWriter<Long> longWriter = new FixedIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        TypeStrategies.LONG,
        ByteOrder.nativeOrder(),
        Long.BYTES,
        true
    );
    longWriter.open();
    longWriter.write(1L);
    longWriter.write(2L);
    longWriter.write(3L);
    longWriter.write(5L);
    longWriter.write(100L);
    longWriter.write(300L);
    longWriter.write(9000L);
    writeToBuffer(longBuffer, longWriter);

    FixedIndexedWriter<Double> doubleWriter = new FixedIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        TypeStrategies.DOUBLE,
        ByteOrder.nativeOrder(),
        Double.BYTES,
        true
    );
    doubleWriter.open();
    doubleWriter.write(1.0);
    doubleWriter.write(1.1);
    doubleWriter.write(1.2);
    doubleWriter.write(2.0);
    doubleWriter.write(2.5);
    doubleWriter.write(3.3);
    doubleWriter.write(6.6);
    doubleWriter.write(9.9);
    writeToBuffer(doubleBuffer, doubleWriter);

    globalStrings = GenericIndexed.read(stringBuffer, GenericIndexed.STRING_STRATEGY);
    globalLongs = FixedIndexed.read(longBuffer, TypeStrategies.LONG, ByteOrder.nativeOrder(), Long.BYTES);
    globalDoubles = FixedIndexed.read(doubleBuffer, TypeStrategies.DOUBLE, ByteOrder.nativeOrder(), Double.BYTES);
  }

  @Test
  public void testSingleTypeStringColumnValueIndex() throws IOException
  {
    NestedFieldLiteralColumnIndexSupplier indexSupplier = makeSingleTypeStringSupplier();

    NullValueIndex nullIndex = indexSupplier.as(NullValueIndex.class);
    Assert.assertNotNull(nullIndex);

    // 10 rows
    // local: [b, foo, fooo, z]
    // column: [foo, b, fooo, b, z, fooo, z, b, b, foo]

    BitmapColumnIndex columnIndex = nullIndex.forNull();
    Assert.assertNotNull(columnIndex);
    Assert.assertEquals(0.0, columnIndex.estimateSelectivity(10), 0.0);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory);
    Assert.assertEquals(0, bitmap.size());
  }

  @Test
  public void testSingleTypeStringColumnValueSetIndex() throws IOException
  {
    NestedFieldLiteralColumnIndexSupplier indexSupplier = makeSingleTypeStringSupplier();

    StringValueSetIndex valueSetIndex = indexSupplier.as(StringValueSetIndex.class);
    Assert.assertNotNull(valueSetIndex);

    // 10 rows
    // local: [b, foo, fooo, z]
    // column: [foo, b, fooo, b, z, fooo, z, b, b, foo]

    BitmapColumnIndex columnIndex = valueSetIndex.forValue("b");
    Assert.assertNotNull(columnIndex);
    Assert.assertEquals(0.4, columnIndex.estimateSelectivity(10), 0.0);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 1, 3, 7, 8);

    // non-existent in local column
    columnIndex = valueSetIndex.forValue("fo");
    Assert.assertNotNull(columnIndex);
    Assert.assertEquals(0.0, columnIndex.estimateSelectivity(10), 0.0);
    bitmap = columnIndex.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap);

    // set index
    columnIndex = valueSetIndex.forSortedValues(new TreeSet<>(ImmutableSet.of("b", "fooo", "z")));
    Assert.assertNotNull(columnIndex);
    Assert.assertEquals(0.8, columnIndex.estimateSelectivity(10), 0.0);
    bitmap = columnIndex.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 1, 2, 3, 4, 5, 6, 7, 8);
  }

  @Test
  public void testSingleTypeStringColumnRangeIndex() throws IOException
  {
    NestedFieldLiteralColumnIndexSupplier indexSupplier = makeSingleTypeStringSupplier();

    LexicographicalRangeIndex rangeIndex = indexSupplier.as(LexicographicalRangeIndex.class);
    Assert.assertNotNull(rangeIndex);

    // 10 rows
    // local: [b, foo, fooo, z]
    // column: [foo, b, fooo, b, z, fooo, z, b, b, foo]

    BitmapColumnIndex forRange = rangeIndex.forRange("f", true, "g", true);
    Assert.assertNotNull(forRange);
    Assert.assertEquals(0.4, forRange.estimateSelectivity(10), 0.0);
    ImmutableBitmap bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 2, 5, 9);

    forRange = rangeIndex.forRange(null, false, "g", true);
    Assert.assertNotNull(forRange);
    Assert.assertEquals(0.8, forRange.estimateSelectivity(10), 0.0);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 1, 2, 3, 5, 7, 8, 9);

    forRange = rangeIndex.forRange("f", false, null, true);
    Assert.assertNotNull(forRange);
    Assert.assertEquals(0.6, forRange.estimateSelectivity(10), 0.0);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 2, 4, 5, 6, 9);

    forRange = rangeIndex.forRange("b", true, "fooo", true);
    Assert.assertEquals(0.2, forRange.estimateSelectivity(10), 0.0);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 9);

    forRange = rangeIndex.forRange("b", true, "fooo", false);
    Assert.assertEquals(0.4, forRange.estimateSelectivity(10), 0.0);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 2, 5, 9);

    forRange = rangeIndex.forRange(null, true, "fooo", true);
    Assert.assertEquals(0.6, forRange.estimateSelectivity(10), 0.0);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 1, 3, 7, 8, 9);

    forRange = rangeIndex.forRange("b", true, null, false);
    Assert.assertEquals(0.6, forRange.estimateSelectivity(10), 0.0);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 2, 4, 5, 6, 9);

    forRange = rangeIndex.forRange("b", false, null, true);
    Assert.assertEquals(1.0, forRange.estimateSelectivity(10), 0.0);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

    forRange = rangeIndex.forRange(null, true, "fooo", false);
    Assert.assertEquals(0.8, forRange.estimateSelectivity(10), 0.0);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 1, 2, 3, 5, 7, 8, 9);

    forRange = rangeIndex.forRange(null, true, null, true);
    Assert.assertEquals(1.0, forRange.estimateSelectivity(10), 0.0);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

    forRange = rangeIndex.forRange(null, false, null, false);
    Assert.assertEquals(1.0, forRange.estimateSelectivity(10), 0.0);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
  }

  @Test
  public void testSingleTypeStringColumnRangeIndexWithPredicate() throws IOException
  {
    NestedFieldLiteralColumnIndexSupplier indexSupplier = makeSingleTypeStringSupplier();

    LexicographicalRangeIndex rangeIndex = indexSupplier.as(LexicographicalRangeIndex.class);
    Assert.assertNotNull(rangeIndex);

    // 10 rows
    // local: [b, foo, fooo, z]
    // column: [foo, b, fooo, b, z, fooo, z, b, b, foo]

    BitmapColumnIndex forRange = rangeIndex.forRange(
        "f",
        true,
        "g",
        true,
        s -> !"fooo".equals(s)
    );
    Assert.assertEquals(0.2, forRange.estimateSelectivity(10), 0.0);
    ImmutableBitmap bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 9);

    forRange = rangeIndex.forRange(
        "f",
        true,
        "g",
        true,
        s -> "fooo".equals(s)
    );
    Assert.assertEquals(0.2, forRange.estimateSelectivity(10), 0.0);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 2, 5);

    forRange = rangeIndex.forRange(
        null,
        false,
        "z",
        false,
        s -> !"fooo".equals(s)
    );
    Assert.assertEquals(0.8, forRange.estimateSelectivity(10), 0.0);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 1, 3, 4, 6, 7, 8, 9);

    forRange = rangeIndex.forRange(
        null,
        false,
        "z",
        true,
        s -> !"fooo".equals(s)
    );
    Assert.assertEquals(0.6, forRange.estimateSelectivity(10), 0.0);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 1, 3, 7, 8, 9);

    forRange = rangeIndex.forRange(
        "f",
        true,
        null,
        true,
        s -> true
    );
    Assert.assertEquals(0.6, forRange.estimateSelectivity(10), 0.0);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 2, 4, 5, 6, 9);
  }

  @Test
  public void testSingleTypeStringColumnPredicateIndex() throws IOException
  {
    NestedFieldLiteralColumnIndexSupplier indexSupplier = makeSingleTypeStringSupplier();

    DruidPredicateIndex predicateIndex = indexSupplier.as(DruidPredicateIndex.class);
    Assert.assertNotNull(predicateIndex);
    DruidPredicateFactory predicateFactory = new InDimFilter.InFilterDruidPredicateFactory(
        null,
        new InDimFilter.ValuesSet(ImmutableSet.of("b", "z"))
    );

    // 10 rows
    // local: [b, foo, fooo, z]
    // column: [foo, b, fooo, b, z, fooo, z, b, b, foo]

    BitmapColumnIndex columnIndex = predicateIndex.forPredicate(predicateFactory);
    Assert.assertNotNull(columnIndex);
    Assert.assertEquals(0.6, columnIndex.estimateSelectivity(10), 0.0);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 1, 3, 4, 6, 7, 8);
  }

  @Test
  public void testSingleTypeStringColumnWithNullValueIndex() throws IOException
  {
    NestedFieldLiteralColumnIndexSupplier indexSupplier = makeSingleTypeStringWithNullsSupplier();

    NullValueIndex nullIndex = indexSupplier.as(NullValueIndex.class);
    Assert.assertNotNull(nullIndex);

    // 10 rows
    // local: [null, b, foo, fooo, z]
    // column: [foo, null, fooo, b, z, fooo, z, null, null, foo]

    BitmapColumnIndex columnIndex = nullIndex.forNull();
    Assert.assertNotNull(columnIndex);
    Assert.assertEquals(0.3, columnIndex.estimateSelectivity(10), 0.0);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 1, 7, 8);
  }

  @Test
  public void testSingleTypeStringColumnWithNullValueSetIndex() throws IOException
  {
    NestedFieldLiteralColumnIndexSupplier indexSupplier = makeSingleTypeStringWithNullsSupplier();

    StringValueSetIndex valueSetIndex = indexSupplier.as(StringValueSetIndex.class);
    Assert.assertNotNull(valueSetIndex);

    // 10 rows
    // local: [null, b, foo, fooo, z]
    // column: [foo, null, fooo, b, z, fooo, z, null, null, foo]

    BitmapColumnIndex columnIndex = valueSetIndex.forValue("b");
    Assert.assertNotNull(columnIndex);
    Assert.assertEquals(0.1, columnIndex.estimateSelectivity(10), 0.0);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 3);

    // non-existent in local column
    columnIndex = valueSetIndex.forValue("fo");
    Assert.assertNotNull(columnIndex);
    Assert.assertEquals(0.0, columnIndex.estimateSelectivity(10), 0.0);
    bitmap = columnIndex.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap);

    // set index
    columnIndex = valueSetIndex.forSortedValues(new TreeSet<>(ImmutableSet.of("b", "fooo", "z")));
    Assert.assertNotNull(columnIndex);
    Assert.assertEquals(0.5, columnIndex.estimateSelectivity(10), 0.0);
    bitmap = columnIndex.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 2, 3, 4, 5, 6);
  }

  @Test
  public void testSingleValueStringWithNullRangeIndex() throws IOException
  {
    NestedFieldLiteralColumnIndexSupplier indexSupplier = makeSingleTypeStringWithNullsSupplier();

    LexicographicalRangeIndex rangeIndex = indexSupplier.as(LexicographicalRangeIndex.class);
    Assert.assertNotNull(rangeIndex);

    // 10 rows
    // local: [null, b, foo, fooo, z]
    // column: [foo, null, fooo, b, z, fooo, z, null, null, foo]

    BitmapColumnIndex forRange = rangeIndex.forRange("f", true, "g", true);
    Assert.assertNotNull(forRange);
    Assert.assertEquals(0.4, forRange.estimateSelectivity(10), 0.0);

    ImmutableBitmap bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 2, 5, 9);

    forRange = rangeIndex.forRange(null, false, "g", true);
    Assert.assertNotNull(forRange);
    Assert.assertEquals(0.5, forRange.estimateSelectivity(10), 0.0);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 2, 3, 5, 9);

    forRange = rangeIndex.forRange("f", false, null, true);
    Assert.assertNotNull(forRange);
    Assert.assertEquals(0.6, forRange.estimateSelectivity(10), 0.0);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 2, 4, 5, 6, 9);

    forRange = rangeIndex.forRange("b", true, "fooo", true);
    Assert.assertEquals(0.2, forRange.estimateSelectivity(10), 0.0);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 9);

    forRange = rangeIndex.forRange("b", true, "fooo", false);
    Assert.assertEquals(0.4, forRange.estimateSelectivity(10), 0.0);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 2, 5, 9);

    forRange = rangeIndex.forRange(null, true, "fooo", true);
    Assert.assertEquals(0.3, forRange.estimateSelectivity(10), 0.0);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 3, 9);

    forRange = rangeIndex.forRange("b", true, null, false);
    Assert.assertEquals(0.6, forRange.estimateSelectivity(10), 0.0);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 2, 4, 5, 6, 9);

    forRange = rangeIndex.forRange("b", false, null, true);
    Assert.assertEquals(0.7, forRange.estimateSelectivity(10), 0.0);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 2, 3, 4, 5, 6, 9);

    forRange = rangeIndex.forRange(null, true, "fooo", false);
    Assert.assertEquals(0.5, forRange.estimateSelectivity(10), 0.0);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 2, 3, 5, 9);

    forRange = rangeIndex.forRange(null, true, null, true);
    Assert.assertEquals(0.7, forRange.estimateSelectivity(10), 0.0);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 2, 3, 4, 5, 6, 9);

    forRange = rangeIndex.forRange(null, false, null, false);
    Assert.assertEquals(0.7, forRange.estimateSelectivity(10), 0.0);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 2, 3, 4, 5, 6, 9);
  }

  @Test
  public void testSingleValueStringWithNullPredicateIndex() throws IOException
  {
    NestedFieldLiteralColumnIndexSupplier indexSupplier = makeSingleTypeStringWithNullsSupplier();

    DruidPredicateIndex predicateIndex = indexSupplier.as(DruidPredicateIndex.class);
    Assert.assertNotNull(predicateIndex);
    DruidPredicateFactory predicateFactory = new InDimFilter.InFilterDruidPredicateFactory(
        null,
        new InDimFilter.ValuesSet(ImmutableSet.of("b", "z"))
    );

    // 10 rows
    // local: [null, b, foo, fooo, z]
    // column: [foo, null, fooo, b, z, fooo, z, null, null, foo]

    BitmapColumnIndex columnIndex = predicateIndex.forPredicate(predicateFactory);
    Assert.assertNotNull(columnIndex);
    Assert.assertEquals(0.3, columnIndex.estimateSelectivity(10), 0.0);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 3, 4, 6);
  }

  @Test
  public void testSingleTypeLongColumnValueSetIndex() throws IOException
  {
    NestedFieldLiteralColumnIndexSupplier indexSupplier = makeSingleTypeLongSupplier();

    StringValueSetIndex valueSetIndex = indexSupplier.as(StringValueSetIndex.class);
    Assert.assertNotNull(valueSetIndex);

    // 10 rows
    // local: [1, 3, 100, 300]
    // column: [100, 1, 300, 1, 3, 3, 100, 300, 300, 1]

    BitmapColumnIndex columnIndex = valueSetIndex.forValue("1");
    Assert.assertNotNull(columnIndex);
    Assert.assertEquals(0.3, columnIndex.estimateSelectivity(10), 0.0);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 1, 3, 9);

    // set index
    columnIndex = valueSetIndex.forSortedValues(new TreeSet<>(ImmutableSet.of("1", "300", "700")));
    Assert.assertNotNull(columnIndex);
    Assert.assertEquals(0.6, columnIndex.estimateSelectivity(10), 0.0);
    bitmap = columnIndex.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 1, 2, 3, 7, 8, 9);
  }

  @Test
  public void testSingleTypeLongColumnRangeIndex() throws IOException
  {
    NestedFieldLiteralColumnIndexSupplier indexSupplier = makeSingleTypeLongSupplier();

    NumericRangeIndex rangeIndex = indexSupplier.as(NumericRangeIndex.class);
    Assert.assertNotNull(rangeIndex);

    // 10 rows
    // local: [1, 3, 100, 300]
    // column: [100, 1, 300, 1, 3, 3, 100, 300, 300, 1]

    BitmapColumnIndex forRange = rangeIndex.forRange(10L, true, 400L, true);
    Assert.assertNotNull(forRange);
    Assert.assertEquals(0.5, forRange.estimateSelectivity(10), 0.0);

    ImmutableBitmap bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 2, 6, 7, 8);

    forRange = rangeIndex.forRange(null, true, null, true);
    Assert.assertEquals(1.0, forRange.estimateSelectivity(10), 0.0);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

    forRange = rangeIndex.forRange(null, false, null, false);
    Assert.assertEquals(1.0, forRange.estimateSelectivity(10), 0.0);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
  }

  @Test
  public void testSingleTypeLongColumnPredicateIndex() throws IOException
  {
    NestedFieldLiteralColumnIndexSupplier indexSupplier = makeSingleTypeLongSupplier();

    DruidPredicateIndex predicateIndex = indexSupplier.as(DruidPredicateIndex.class);
    Assert.assertNotNull(predicateIndex);
    DruidPredicateFactory predicateFactory = new InDimFilter.InFilterDruidPredicateFactory(
        null,
        new InDimFilter.ValuesSet(ImmutableSet.of("1", "3"))
    );

    // 10 rows
    // local: [1, 3, 100, 300]
    // column: [100, 1, 300, 1, 3, 3, 100, 300, 300, 1]

    BitmapColumnIndex columnIndex = predicateIndex.forPredicate(predicateFactory);
    Assert.assertNotNull(columnIndex);
    Assert.assertEquals(0.5, columnIndex.estimateSelectivity(10), 0.0);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 1, 3, 4, 5, 9);
  }

  @Test
  public void testSingleTypeLongColumnWithNullValueIndex() throws IOException
  {
    NestedFieldLiteralColumnIndexSupplier indexSupplier = makeSingleTypeLongSupplierWithNull();

    NullValueIndex nullIndex = indexSupplier.as(NullValueIndex.class);
    Assert.assertNotNull(nullIndex);

    // 10 rows
    // local: [null, 1, 3, 100, 300]
    // column: [100, 1, null, 1, 3, null, 100, 300, null, 1]

    BitmapColumnIndex columnIndex = nullIndex.forNull();
    Assert.assertNotNull(columnIndex);
    Assert.assertEquals(0.3, columnIndex.estimateSelectivity(10), 0.0);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 2, 5, 8);
  }

  @Test
  public void testSingleTypeLongColumnWithNullValueSetIndex() throws IOException
  {
    NestedFieldLiteralColumnIndexSupplier indexSupplier = makeSingleTypeLongSupplierWithNull();

    StringValueSetIndex valueSetIndex = indexSupplier.as(StringValueSetIndex.class);
    Assert.assertNotNull(valueSetIndex);

    // 10 rows
    // local: [null, 1, 3, 100, 300]
    // column: [100, 1, null, 1, 3, null, 100, 300, null, 1]

    BitmapColumnIndex columnIndex = valueSetIndex.forValue("3");
    Assert.assertNotNull(columnIndex);
    Assert.assertEquals(0.1, columnIndex.estimateSelectivity(10), 0.0);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 4);

    // set index
    columnIndex = valueSetIndex.forSortedValues(new TreeSet<>(ImmutableSet.of("1", "3", "300")));
    Assert.assertNotNull(columnIndex);
    Assert.assertEquals(0.5, columnIndex.estimateSelectivity(10), 0.0);
    bitmap = columnIndex.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 1, 3, 4, 7, 9);
  }

  @Test
  public void testSingleValueLongWithNullRangeIndex() throws IOException
  {
    NestedFieldLiteralColumnIndexSupplier indexSupplier = makeSingleTypeLongSupplierWithNull();

    NumericRangeIndex rangeIndex = indexSupplier.as(NumericRangeIndex.class);
    Assert.assertNotNull(rangeIndex);

    // 10 rows
    // local: [null, 1, 3, 100, 300]
    // column: [100, 1, null, 1, 3, null, 100, 300, null, 1]

    BitmapColumnIndex forRange = rangeIndex.forRange(100, false, 700, true);
    Assert.assertNotNull(forRange);
    Assert.assertEquals(0.3, forRange.estimateSelectivity(10), 0.0);

    ImmutableBitmap bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 6, 7);

    forRange = rangeIndex.forRange(null, true, null, true);
    Assert.assertEquals(0.7, forRange.estimateSelectivity(10), 0.0);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 1, 3, 4, 6, 7, 9);

    forRange = rangeIndex.forRange(null, false, null, false);
    Assert.assertEquals(0.7, forRange.estimateSelectivity(10), 0.0);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 1, 3, 4, 6, 7, 9);
  }

  @Test
  public void testSingleValueLongWithNullPredicateIndex() throws IOException
  {
    NestedFieldLiteralColumnIndexSupplier indexSupplier = makeSingleTypeLongSupplierWithNull();

    DruidPredicateIndex predicateIndex = indexSupplier.as(DruidPredicateIndex.class);
    Assert.assertNotNull(predicateIndex);
    DruidPredicateFactory predicateFactory = new InDimFilter.InFilterDruidPredicateFactory(
        null,
        new InDimFilter.ValuesSet(ImmutableSet.of("3", "100"))
    );

    // 10 rows
    // local: [null, 1, 3, 100, 300]
    // column: [100, 1, null, 1, 3, null, 100, 300, null, 1]

    BitmapColumnIndex columnIndex = predicateIndex.forPredicate(predicateFactory);
    Assert.assertNotNull(columnIndex);
    Assert.assertEquals(0.3, columnIndex.estimateSelectivity(10), 0.0);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 4, 6);
  }

  @Test
  public void testSingleTypeDoubleColumnValueSetIndex() throws IOException
  {
    NestedFieldLiteralColumnIndexSupplier indexSupplier = makeSingleTypeDoubleSupplier();

    StringValueSetIndex valueSetIndex = indexSupplier.as(StringValueSetIndex.class);
    Assert.assertNotNull(valueSetIndex);

    // 10 rows
    // local: [1.1, 1.2, 3.3, 6.6]
    // column: [1.1, 1.1, 1.2, 3.3, 1.2, 6.6, 3.3, 1.2, 1.1, 3.3]

    BitmapColumnIndex columnIndex = valueSetIndex.forValue("1.2");
    Assert.assertNotNull(columnIndex);
    Assert.assertEquals(0.3, columnIndex.estimateSelectivity(10), 0.0);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 2, 4, 7);

    // set index
    columnIndex = valueSetIndex.forSortedValues(new TreeSet<>(ImmutableSet.of("1.2", "3.3", "6.6")));
    Assert.assertNotNull(columnIndex);
    Assert.assertEquals(0.7, columnIndex.estimateSelectivity(10), 0.0);
    bitmap = columnIndex.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 2, 3, 4, 5, 6, 7, 9);
  }

  @Test
  public void testSingleTypeDoubleColumnRangeIndex() throws IOException
  {
    NestedFieldLiteralColumnIndexSupplier indexSupplier = makeSingleTypeDoubleSupplier();

    NumericRangeIndex rangeIndex = indexSupplier.as(NumericRangeIndex.class);
    Assert.assertNotNull(rangeIndex);

    // 10 rows
    // local: [1.1, 1.2, 3.3, 6.6]
    // column: [1.1, 1.1, 1.2, 3.3, 1.2, 6.6, 3.3, 1.2, 1.1, 3.3]

    BitmapColumnIndex forRange = rangeIndex.forRange(1.0, true, 5.0, true);
    Assert.assertNotNull(forRange);
    Assert.assertEquals(0.9, forRange.estimateSelectivity(10), 0.0);

    ImmutableBitmap bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 1, 2, 3, 4, 6, 7, 8, 9);

    forRange = rangeIndex.forRange(1.1, false, 3.3, false);
    Assert.assertNotNull(forRange);
    Assert.assertEquals(0.9, forRange.estimateSelectivity(10), 0.0);

    bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 1, 2, 3, 4, 6, 7, 8, 9);

    forRange = rangeIndex.forRange(1.1, true, 3.3, true);
    Assert.assertNotNull(forRange);
    Assert.assertEquals(0.6, forRange.estimateSelectivity(10), 0.0);

    bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 2, 3, 4, 6, 7, 9);

    forRange = rangeIndex.forRange(null, true, null, true);
    Assert.assertEquals(1.0, forRange.estimateSelectivity(10), 0.0);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

    forRange = rangeIndex.forRange(null, false, null, false);
    Assert.assertEquals(1.0, forRange.estimateSelectivity(10), 0.0);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
  }

  @Test
  public void testSingleTypeDoubleColumnPredicateIndex() throws IOException
  {
    NestedFieldLiteralColumnIndexSupplier indexSupplier = makeSingleTypeDoubleSupplier();

    DruidPredicateIndex predicateIndex = indexSupplier.as(DruidPredicateIndex.class);
    Assert.assertNotNull(predicateIndex);
    DruidPredicateFactory predicateFactory = new InDimFilter.InFilterDruidPredicateFactory(
        null,
        new InDimFilter.ValuesSet(ImmutableSet.of("1.2", "3.3", "5.0"))
    );

    // 10 rows
    // local: [1.1, 1.2, 3.3, 6.6]
    // column: [1.1, 1.1, 1.2, 3.3, 1.2, 6.6, 3.3, 1.2, 1.1, 3.3]

    BitmapColumnIndex columnIndex = predicateIndex.forPredicate(predicateFactory);
    Assert.assertNotNull(columnIndex);
    Assert.assertEquals(0.6, columnIndex.estimateSelectivity(10), 0.0);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 2, 3, 4, 6, 7, 9);
  }

  @Test
  public void testSingleTypeDoubleColumnWithNullValueIndex() throws IOException
  {
    NestedFieldLiteralColumnIndexSupplier indexSupplier = makeSingleTypeDoubleSupplierWithNull();

    NullValueIndex nullIndex = indexSupplier.as(NullValueIndex.class);
    Assert.assertNotNull(nullIndex);

    // 10 rows
    // local: [null, 1.1, 1.2, 3.3, 6.6]
    // column: [1.1, null, 1.2, null, 1.2, 6.6, null, 1.2, 1.1, 3.3]

    BitmapColumnIndex columnIndex = nullIndex.forNull();
    Assert.assertNotNull(columnIndex);
    Assert.assertEquals(0.3, columnIndex.estimateSelectivity(10), 0.0);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 1, 3, 6);
  }

  @Test
  public void testSingleTypeDoubleColumnWithNullValueSetIndex() throws IOException
  {
    NestedFieldLiteralColumnIndexSupplier indexSupplier = makeSingleTypeDoubleSupplierWithNull();

    StringValueSetIndex valueSetIndex = indexSupplier.as(StringValueSetIndex.class);
    Assert.assertNotNull(valueSetIndex);

    // 10 rows
    // local: [null, 1.1, 1.2, 3.3, 6.6]
    // column: [1.1, null, 1.2, null, 1.2, 6.6, null, 1.2, 1.1, 3.3]

    BitmapColumnIndex columnIndex = valueSetIndex.forValue("6.6");
    Assert.assertNotNull(columnIndex);
    Assert.assertEquals(0.1, columnIndex.estimateSelectivity(10), 0.0);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 5);

    // set index
    columnIndex = valueSetIndex.forSortedValues(new TreeSet<>(ImmutableSet.of("1.2", "3.3", "7.7")));
    Assert.assertNotNull(columnIndex);
    Assert.assertEquals(0.4, columnIndex.estimateSelectivity(10), 0.0);
    bitmap = columnIndex.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 2, 4, 7, 9);
  }

  @Test
  public void testSingleValueDoubleWithNullRangeIndex() throws IOException
  {
    NestedFieldLiteralColumnIndexSupplier indexSupplier = makeSingleTypeDoubleSupplierWithNull();

    NumericRangeIndex rangeIndex = indexSupplier.as(NumericRangeIndex.class);
    Assert.assertNotNull(rangeIndex);

    // 10 rows
    // local: [null, 1.1, 1.2, 3.3, 6.6]
    // column: [1.1, null, 1.2, null, 1.2, 6.6, null, 1.2, 1.1, 3.3]

    BitmapColumnIndex forRange = rangeIndex.forRange(1.1, false, 5.0, true);
    Assert.assertNotNull(forRange);
    Assert.assertEquals(0.6, forRange.estimateSelectivity(10), 0.0);

    ImmutableBitmap bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 2, 4, 7, 8, 9);

    forRange = rangeIndex.forRange(null, true, null, true);
    Assert.assertEquals(0.7, forRange.estimateSelectivity(10), 0.0);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 2, 4, 5, 7, 8, 9);

    forRange = rangeIndex.forRange(null, false, null, false);
    Assert.assertEquals(0.7, forRange.estimateSelectivity(10), 0.0);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 2, 4, 5, 7, 8, 9);
  }

  @Test
  public void testSingleValueDoubleWithNullPredicateIndex() throws IOException
  {
    NestedFieldLiteralColumnIndexSupplier indexSupplier = makeSingleTypeDoubleSupplierWithNull();

    DruidPredicateIndex predicateIndex = indexSupplier.as(DruidPredicateIndex.class);
    Assert.assertNotNull(predicateIndex);
    DruidPredicateFactory predicateFactory = new InDimFilter.InFilterDruidPredicateFactory(
        null,
        new InDimFilter.ValuesSet(ImmutableSet.of("1.2", "3.3"))
    );

    // 10 rows
    // local: [null, 1.1, 1.2, 3.3, 6.6]
    // column: [1.1, null, 1.2, null, 1.2, 6.6, null, 1.2, 1.1, 3.3]

    BitmapColumnIndex columnIndex = predicateIndex.forPredicate(predicateFactory);
    Assert.assertNotNull(columnIndex);
    Assert.assertEquals(0.4, columnIndex.estimateSelectivity(10), 0.0);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 2, 4, 7, 9);
  }

  @Test
  public void testVariantNullValueIndex() throws IOException
  {
    NestedFieldLiteralColumnIndexSupplier indexSupplier = makeVariantSupplierWithNull();

    NullValueIndex nullIndex = indexSupplier.as(NullValueIndex.class);
    Assert.assertNotNull(nullIndex);

    // 10 rows
    // local: [null, b, z, 1, 300, 1.1, 9.9]
    // column: [1, b, null, 9.9, 300, 1, z, null, 1.1, b]

    BitmapColumnIndex columnIndex = nullIndex.forNull();
    Assert.assertNotNull(columnIndex);
    Assert.assertEquals(0.2, columnIndex.estimateSelectivity(10), 0.0);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 2, 7);
  }

  @Test
  public void testVariantValueSetIndex() throws IOException
  {
    NestedFieldLiteralColumnIndexSupplier indexSupplier = makeVariantSupplierWithNull();

    StringValueSetIndex valueSetIndex = indexSupplier.as(StringValueSetIndex.class);
    Assert.assertNotNull(valueSetIndex);

    // 10 rows
    // local: [null, b, z, 1, 300, 1.1, 9.9]
    // column: [1, b, null, 9.9, 300, 1, z, null, 1.1, b]

    BitmapColumnIndex columnIndex = valueSetIndex.forValue("b");
    Assert.assertNotNull(columnIndex);
    Assert.assertEquals(0.2, columnIndex.estimateSelectivity(10), 0.0);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 1, 9);

    columnIndex = valueSetIndex.forValue("1");
    Assert.assertNotNull(columnIndex);
    Assert.assertEquals(0.2, columnIndex.estimateSelectivity(10), 0.0);
    bitmap = columnIndex.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 0, 5);

    columnIndex = valueSetIndex.forValue("1.1");
    Assert.assertNotNull(columnIndex);
    Assert.assertEquals(0.1, columnIndex.estimateSelectivity(10), 0.0);
    bitmap = columnIndex.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 8);

    // set index
    columnIndex = valueSetIndex.forSortedValues(new TreeSet<>(ImmutableSet.of("b", "300", "9.9", "1.6")));
    Assert.assertNotNull(columnIndex);
    Assert.assertEquals(0.4, columnIndex.estimateSelectivity(10), 0.0);
    bitmap = columnIndex.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 1, 3, 4, 9);
  }

  @Test
  public void testVariantRangeIndex() throws IOException
  {
    NestedFieldLiteralColumnIndexSupplier indexSupplier = makeVariantSupplierWithNull();

    LexicographicalRangeIndex rangeIndex = indexSupplier.as(LexicographicalRangeIndex.class);
    Assert.assertNull(rangeIndex);

    NumericRangeIndex numericRangeIndex = indexSupplier.as(NumericRangeIndex.class);
    Assert.assertNull(numericRangeIndex);
  }

  @Test
  public void testVariantPredicateIndex() throws IOException
  {
    NestedFieldLiteralColumnIndexSupplier indexSupplier = makeVariantSupplierWithNull();

    DruidPredicateIndex predicateIndex = indexSupplier.as(DruidPredicateIndex.class);
    Assert.assertNotNull(predicateIndex);
    DruidPredicateFactory predicateFactory = new InDimFilter.InFilterDruidPredicateFactory(
        null,
        new InDimFilter.ValuesSet(ImmutableSet.of("b", "z", "9.9", "300"))
    );

    // 10 rows
    // local: [null, b, z, 1, 300, 1.1, 9.9]
    // column: [1, b, null, 9.9, 300, 1, z, null, 1.1, b]

    BitmapColumnIndex columnIndex = predicateIndex.forPredicate(predicateFactory);
    Assert.assertNotNull(columnIndex);
    Assert.assertEquals(0.5, columnIndex.estimateSelectivity(10), 0.0);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory);
    checkBitmap(bitmap, 1, 3, 4, 6, 9);
  }

  @Test
  public void testDictionaryEncodedStringValueIndex() throws IOException
  {
    NestedFieldLiteralColumnIndexSupplier indexSupplier = makeVariantSupplierWithNull();

    DictionaryEncodedStringValueIndex lowLevelIndex = indexSupplier.as(DictionaryEncodedStringValueIndex.class);
    Assert.assertNotNull(lowLevelIndex);
    Assert.assertNotNull(indexSupplier.as(DictionaryEncodedValueIndex.class));

    // 10 rows
    // local: [null, b, z, 1, 300, 1.1, 9.9]
    // column: [1, b, null, 9.9, 300, 1, z, null, 1.1, b]

    Assert.assertNull(lowLevelIndex.getValue(0));
    Assert.assertEquals("b", lowLevelIndex.getValue(1));
    Assert.assertEquals("z", lowLevelIndex.getValue(2));
    Assert.assertEquals("1", lowLevelIndex.getValue(3));
    Assert.assertEquals("300", lowLevelIndex.getValue(4));
    Assert.assertEquals("1.1", lowLevelIndex.getValue(5));
    Assert.assertEquals("9.9", lowLevelIndex.getValue(6));
  }

  private NestedFieldLiteralColumnIndexSupplier makeSingleTypeStringSupplier() throws IOException
  {
    ByteBuffer localDictionaryBuffer = ByteBuffer.allocate(1 << 12).order(ByteOrder.nativeOrder());
    ByteBuffer bitmapsBuffer = ByteBuffer.allocate(1 << 12);

    FixedIndexedWriter<Integer> localDictionaryWriter = new FixedIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        NestedDataColumnSerializer.INT_TYPE_STRATEGY,
        ByteOrder.nativeOrder(),
        Integer.BYTES,
        true
    );
    localDictionaryWriter.open();
    GenericIndexedWriter<ImmutableBitmap> bitmapWriter = new GenericIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        "bitmaps",
        roaringFactory.getObjectStrategy()
    );
    bitmapWriter.setObjectsNotSorted();
    bitmapWriter.open();

    // 10 rows
    // globals: [
    //    [null, a, b, fo, foo, fooo, z],
    //    [1, 2, 3, 5, 100, 300, 9000],
    //    [1.0, 1.1, 1.2, 2.0, 2.5, 3.3, 6.6, 9.9]
    // ]
    // local: [b, foo, fooo, z]
    // column: [foo, b, fooo, b, z, fooo, z, b, b, foo]

    // b
    localDictionaryWriter.write(2);
    bitmapWriter.write(fillBitmap(1, 3, 7, 8));

    // foo
    localDictionaryWriter.write(4);
    bitmapWriter.write(fillBitmap(0, 9));

    // fooo
    localDictionaryWriter.write(5);
    bitmapWriter.write(fillBitmap(2, 5));

    // z
    localDictionaryWriter.write(6);
    bitmapWriter.write(fillBitmap(4, 6));

    writeToBuffer(localDictionaryBuffer, localDictionaryWriter);
    writeToBuffer(bitmapsBuffer, bitmapWriter);

    FixedIndexed<Integer> dictionary = FixedIndexed.read(
        localDictionaryBuffer,
        NestedDataColumnSerializer.INT_TYPE_STRATEGY,
        ByteOrder.nativeOrder(),
        Integer.BYTES
    );

    GenericIndexed<ImmutableBitmap> bitmaps = GenericIndexed.read(bitmapsBuffer, roaringFactory.getObjectStrategy());

    return new NestedFieldLiteralColumnIndexSupplier(
        new NestedLiteralTypeInfo.TypeSet(
            new NestedLiteralTypeInfo.MutableTypeSet().add(ColumnType.STRING).getByteValue()
        ),
        roaringFactory.getBitmapFactory(),
        bitmaps,
        dictionary,
        globalStrings,
        globalLongs,
        globalDoubles
    );
  }

  private NestedFieldLiteralColumnIndexSupplier makeSingleTypeStringWithNullsSupplier() throws IOException
  {
    ByteBuffer localDictionaryBuffer = ByteBuffer.allocate(1 << 12).order(ByteOrder.nativeOrder());
    ByteBuffer bitmapsBuffer = ByteBuffer.allocate(1 << 12);

    FixedIndexedWriter<Integer> localDictionaryWriter = new FixedIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        NestedDataColumnSerializer.INT_TYPE_STRATEGY,
        ByteOrder.nativeOrder(),
        Integer.BYTES,
        true
    );
    localDictionaryWriter.open();
    GenericIndexedWriter<ImmutableBitmap> bitmapWriter = new GenericIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        "bitmaps",
        roaringFactory.getObjectStrategy()
    );
    bitmapWriter.setObjectsNotSorted();
    bitmapWriter.open();
    // 10 rows
    // globals: [
    //    [null, a, b, fo, foo, fooo, z],
    //    [1, 2, 3, 5, 100, 300, 9000],
    //    [1.0, 1.1, 1.2, 2.0, 2.5, 3.3, 6.6, 9.9]
    // ]
    // local: [null, b, foo, fooo, z]
    // column: [foo, null, fooo, b, z, fooo, z, null, null, foo]

    // null
    localDictionaryWriter.write(0);
    bitmapWriter.write(fillBitmap(1, 7, 8));

    // b
    localDictionaryWriter.write(2);
    bitmapWriter.write(fillBitmap(3));

    // foo
    localDictionaryWriter.write(4);
    bitmapWriter.write(fillBitmap(0, 9));

    // fooo
    localDictionaryWriter.write(5);
    bitmapWriter.write(fillBitmap(2, 5));

    // z
    localDictionaryWriter.write(6);
    bitmapWriter.write(fillBitmap(4, 6));

    writeToBuffer(localDictionaryBuffer, localDictionaryWriter);
    writeToBuffer(bitmapsBuffer, bitmapWriter);

    FixedIndexed<Integer> dictionary = FixedIndexed.read(
        localDictionaryBuffer,
        NestedDataColumnSerializer.INT_TYPE_STRATEGY,
        ByteOrder.nativeOrder(),
        Integer.BYTES
    );

    GenericIndexed<ImmutableBitmap> bitmaps = GenericIndexed.read(bitmapsBuffer, roaringFactory.getObjectStrategy());

    return new NestedFieldLiteralColumnIndexSupplier(
        new NestedLiteralTypeInfo.TypeSet(
            new NestedLiteralTypeInfo.MutableTypeSet().add(ColumnType.STRING).getByteValue()
        ),
        roaringFactory.getBitmapFactory(),
        bitmaps,
        dictionary,
        globalStrings,
        globalLongs,
        globalDoubles
    );
  }

  private NestedFieldLiteralColumnIndexSupplier makeSingleTypeLongSupplier() throws IOException
  {
    ByteBuffer localDictionaryBuffer = ByteBuffer.allocate(1 << 12).order(ByteOrder.nativeOrder());
    ByteBuffer bitmapsBuffer = ByteBuffer.allocate(1 << 12);

    FixedIndexedWriter<Integer> localDictionaryWriter = new FixedIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        NestedDataColumnSerializer.INT_TYPE_STRATEGY,
        ByteOrder.nativeOrder(),
        Integer.BYTES,
        true
    );
    localDictionaryWriter.open();
    GenericIndexedWriter<ImmutableBitmap> bitmapWriter = new GenericIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        "bitmaps",
        roaringFactory.getObjectStrategy()
    );
    bitmapWriter.setObjectsNotSorted();
    bitmapWriter.open();

    // 10 rows
    // globals: [
    //    [null, a, b, fo, foo, fooo, z],
    //    [1, 2, 3, 5, 100, 300, 9000],
    //    [1.0, 1.1, 1.2, 2.0, 2.5, 3.3, 6.6, 9.9]
    // ]
    // local: [1, 3, 100, 300]
    // column: [100, 1, 300, 1, 3, 3, 100, 300, 300, 1]

    // 1
    localDictionaryWriter.write(7);
    bitmapWriter.write(fillBitmap(1, 3, 9));

    // 3
    localDictionaryWriter.write(9);
    bitmapWriter.write(fillBitmap(4, 5));

    // 100
    localDictionaryWriter.write(11);
    bitmapWriter.write(fillBitmap(0, 6));

    // 300
    localDictionaryWriter.write(12);
    bitmapWriter.write(fillBitmap(2, 7, 8));

    writeToBuffer(localDictionaryBuffer, localDictionaryWriter);
    writeToBuffer(bitmapsBuffer, bitmapWriter);

    FixedIndexed<Integer> dictionary = FixedIndexed.read(
        localDictionaryBuffer,
        NestedDataColumnSerializer.INT_TYPE_STRATEGY,
        ByteOrder.nativeOrder(),
        Integer.BYTES
    );

    GenericIndexed<ImmutableBitmap> bitmaps = GenericIndexed.read(bitmapsBuffer, roaringFactory.getObjectStrategy());

    return new NestedFieldLiteralColumnIndexSupplier(
        new NestedLiteralTypeInfo.TypeSet(
            new NestedLiteralTypeInfo.MutableTypeSet().add(ColumnType.LONG).getByteValue()
        ),
        roaringFactory.getBitmapFactory(),
        bitmaps,
        dictionary,
        globalStrings,
        globalLongs,
        globalDoubles
    );
  }

  private NestedFieldLiteralColumnIndexSupplier makeSingleTypeLongSupplierWithNull() throws IOException
  {
    ByteBuffer localDictionaryBuffer = ByteBuffer.allocate(1 << 12).order(ByteOrder.nativeOrder());
    ByteBuffer bitmapsBuffer = ByteBuffer.allocate(1 << 12);

    FixedIndexedWriter<Integer> localDictionaryWriter = new FixedIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        NestedDataColumnSerializer.INT_TYPE_STRATEGY,
        ByteOrder.nativeOrder(),
        Integer.BYTES,
        true
    );
    localDictionaryWriter.open();
    GenericIndexedWriter<ImmutableBitmap> bitmapWriter = new GenericIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        "bitmaps",
        roaringFactory.getObjectStrategy()
    );
    bitmapWriter.setObjectsNotSorted();
    bitmapWriter.open();

    // 10 rows
    // globals: [
    //    [null, a, b, fo, foo, fooo, z],
    //    [1, 2, 3, 5, 100, 300, 9000],
    //    [1.0, 1.1, 1.2, 2.0, 2.5, 3.3, 6.6, 9.9]
    // ]
    // local: [null, 1, 3, 100, 300]
    // column: [100, 1, null, 1, 3, null, 100, 300, null, 1]

    // null
    localDictionaryWriter.write(0);
    bitmapWriter.write(fillBitmap(2, 5, 8));

    // 1
    localDictionaryWriter.write(7);
    bitmapWriter.write(fillBitmap(1, 3, 9));

    // 3
    localDictionaryWriter.write(9);
    bitmapWriter.write(fillBitmap(4));

    // 100
    localDictionaryWriter.write(11);
    bitmapWriter.write(fillBitmap(0, 6));

    // 300
    localDictionaryWriter.write(12);
    bitmapWriter.write(fillBitmap(7));

    writeToBuffer(localDictionaryBuffer, localDictionaryWriter);
    writeToBuffer(bitmapsBuffer, bitmapWriter);

    FixedIndexed<Integer> dictionary = FixedIndexed.read(
        localDictionaryBuffer,
        NestedDataColumnSerializer.INT_TYPE_STRATEGY,
        ByteOrder.nativeOrder(),
        Integer.BYTES
    );

    GenericIndexed<ImmutableBitmap> bitmaps = GenericIndexed.read(bitmapsBuffer, roaringFactory.getObjectStrategy());

    return new NestedFieldLiteralColumnIndexSupplier(
        new NestedLiteralTypeInfo.TypeSet(
            new NestedLiteralTypeInfo.MutableTypeSet().add(ColumnType.LONG).getByteValue()
        ),
        roaringFactory.getBitmapFactory(),
        bitmaps,
        dictionary,
        globalStrings,
        globalLongs,
        globalDoubles
    );
  }

  private NestedFieldLiteralColumnIndexSupplier makeSingleTypeDoubleSupplier() throws IOException
  {
    ByteBuffer localDictionaryBuffer = ByteBuffer.allocate(1 << 12).order(ByteOrder.nativeOrder());
    ByteBuffer bitmapsBuffer = ByteBuffer.allocate(1 << 12);

    FixedIndexedWriter<Integer> localDictionaryWriter = new FixedIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        NestedDataColumnSerializer.INT_TYPE_STRATEGY,
        ByteOrder.nativeOrder(),
        Integer.BYTES,
        true
    );
    localDictionaryWriter.open();
    GenericIndexedWriter<ImmutableBitmap> bitmapWriter = new GenericIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        "bitmaps",
        roaringFactory.getObjectStrategy()
    );
    bitmapWriter.setObjectsNotSorted();
    bitmapWriter.open();

    // 10 rows
    // globals: [
    //    [null, a, b, fo, foo, fooo, z],
    //    [1, 2, 3, 5, 100, 300, 9000],
    //    [1.0, 1.1, 1.2, 2.0, 2.5, 3.3, 6.6, 9.9]
    // ]
    // local: [1.1, 1.2, 3.3, 6.6]
    // column: [1.1, 1.1, 1.2, 3.3, 1.2, 6.6, 3.3, 1.2, 1.1, 3.3]

    // 1.1
    localDictionaryWriter.write(15);
    bitmapWriter.write(fillBitmap(0, 1, 8));

    // 1.2
    localDictionaryWriter.write(16);
    bitmapWriter.write(fillBitmap(2, 4, 7));

    // 3.3
    localDictionaryWriter.write(19);
    bitmapWriter.write(fillBitmap(3, 6, 9));

    // 6.6
    localDictionaryWriter.write(20);
    bitmapWriter.write(fillBitmap(5));

    writeToBuffer(localDictionaryBuffer, localDictionaryWriter);
    writeToBuffer(bitmapsBuffer, bitmapWriter);

    FixedIndexed<Integer> dictionary = FixedIndexed.read(
        localDictionaryBuffer,
        NestedDataColumnSerializer.INT_TYPE_STRATEGY,
        ByteOrder.nativeOrder(),
        Integer.BYTES
    );

    GenericIndexed<ImmutableBitmap> bitmaps = GenericIndexed.read(bitmapsBuffer, roaringFactory.getObjectStrategy());

    return new NestedFieldLiteralColumnIndexSupplier(
        new NestedLiteralTypeInfo.TypeSet(
            new NestedLiteralTypeInfo.MutableTypeSet().add(ColumnType.DOUBLE).getByteValue()
        ),
        roaringFactory.getBitmapFactory(),
        bitmaps,
        dictionary,
        globalStrings,
        globalLongs,
        globalDoubles
    );
  }

  private NestedFieldLiteralColumnIndexSupplier makeSingleTypeDoubleSupplierWithNull() throws IOException
  {
    ByteBuffer localDictionaryBuffer = ByteBuffer.allocate(1 << 12).order(ByteOrder.nativeOrder());
    ByteBuffer bitmapsBuffer = ByteBuffer.allocate(1 << 12);

    FixedIndexedWriter<Integer> localDictionaryWriter = new FixedIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        NestedDataColumnSerializer.INT_TYPE_STRATEGY,
        ByteOrder.nativeOrder(),
        Integer.BYTES,
        true
    );
    localDictionaryWriter.open();
    GenericIndexedWriter<ImmutableBitmap> bitmapWriter = new GenericIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        "bitmaps",
        roaringFactory.getObjectStrategy()
    );
    bitmapWriter.setObjectsNotSorted();
    bitmapWriter.open();

    // 10 rows
    // globals: [
    //    [null, a, b, fo, foo, fooo, z],
    //    [1, 2, 3, 5, 100, 300, 9000],
    //    [1.0, 1.1, 1.2, 2.0, 2.5, 3.3, 6.6, 9.9]
    // ]
    // local: [null, 1.1, 1.2, 3.3, 6.6]
    // column: [1.1, null, 1.2, null, 1.2, 6.6, null, 1.2, 1.1, 3.3]

    // null
    localDictionaryWriter.write(0);
    bitmapWriter.write(fillBitmap(1, 3, 6));

    // 1.1
    localDictionaryWriter.write(15);
    bitmapWriter.write(fillBitmap(0, 8));

    // 1.2
    localDictionaryWriter.write(16);
    bitmapWriter.write(fillBitmap(2, 4, 7));

    // 3.3
    localDictionaryWriter.write(19);
    bitmapWriter.write(fillBitmap(9));

    // 6.6
    localDictionaryWriter.write(20);
    bitmapWriter.write(fillBitmap(5));

    writeToBuffer(localDictionaryBuffer, localDictionaryWriter);
    writeToBuffer(bitmapsBuffer, bitmapWriter);

    FixedIndexed<Integer> dictionary = FixedIndexed.read(
        localDictionaryBuffer,
        NestedDataColumnSerializer.INT_TYPE_STRATEGY,
        ByteOrder.nativeOrder(),
        Integer.BYTES
    );

    GenericIndexed<ImmutableBitmap> bitmaps = GenericIndexed.read(bitmapsBuffer, roaringFactory.getObjectStrategy());

    return new NestedFieldLiteralColumnIndexSupplier(
        new NestedLiteralTypeInfo.TypeSet(
            new NestedLiteralTypeInfo.MutableTypeSet().add(ColumnType.DOUBLE).getByteValue()
        ),
        roaringFactory.getBitmapFactory(),
        bitmaps,
        dictionary,
        globalStrings,
        globalLongs,
        globalDoubles
    );
  }

  private NestedFieldLiteralColumnIndexSupplier makeVariantSupplierWithNull() throws IOException
  {
    ByteBuffer localDictionaryBuffer = ByteBuffer.allocate(1 << 12).order(ByteOrder.nativeOrder());
    ByteBuffer bitmapsBuffer = ByteBuffer.allocate(1 << 12);

    FixedIndexedWriter<Integer> localDictionaryWriter = new FixedIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        NestedDataColumnSerializer.INT_TYPE_STRATEGY,
        ByteOrder.nativeOrder(),
        Integer.BYTES,
        true
    );
    localDictionaryWriter.open();
    GenericIndexedWriter<ImmutableBitmap> bitmapWriter = new GenericIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        "bitmaps",
        roaringFactory.getObjectStrategy()
    );
    bitmapWriter.setObjectsNotSorted();
    bitmapWriter.open();

    // 10 rows
    // globals: [
    //    [null, a, b, fo, foo, fooo, z],
    //    [1, 2, 3, 5, 100, 300, 9000],
    //    [1.0, 1.1, 1.2, 2.0, 2.5, 3.3, 6.6, 9.9]
    // ]
    // local: [null, b, z, 1, 300, 1.1, 9.9]
    // column: [1, b, null, 9.9, 300, 1, z, null, 1.1, b]

    // null
    localDictionaryWriter.write(0);
    bitmapWriter.write(fillBitmap(2, 7));

    // b
    localDictionaryWriter.write(2);
    bitmapWriter.write(fillBitmap(1, 9));

    // z
    localDictionaryWriter.write(6);
    bitmapWriter.write(fillBitmap(6));

    // 1
    localDictionaryWriter.write(7);
    bitmapWriter.write(fillBitmap(0, 5));

    // 300
    localDictionaryWriter.write(12);
    bitmapWriter.write(fillBitmap(4));

    // 1.1
    localDictionaryWriter.write(15);
    bitmapWriter.write(fillBitmap(8));

    // 9.9
    localDictionaryWriter.write(21);
    bitmapWriter.write(fillBitmap(3));

    writeToBuffer(localDictionaryBuffer, localDictionaryWriter);
    writeToBuffer(bitmapsBuffer, bitmapWriter);

    FixedIndexed<Integer> dictionary = FixedIndexed.read(
        localDictionaryBuffer,
        NestedDataColumnSerializer.INT_TYPE_STRATEGY,
        ByteOrder.nativeOrder(),
        Integer.BYTES
    );

    GenericIndexed<ImmutableBitmap> bitmaps = GenericIndexed.read(bitmapsBuffer, roaringFactory.getObjectStrategy());

    return new NestedFieldLiteralColumnIndexSupplier(
        new NestedLiteralTypeInfo.TypeSet(
            new NestedLiteralTypeInfo.MutableTypeSet().add(ColumnType.STRING)
                                                      .add(ColumnType.LONG)
                                                      .add(ColumnType.DOUBLE)
                                                      .getByteValue()
        ),
        roaringFactory.getBitmapFactory(),
        bitmaps,
        dictionary,
        globalStrings,
        globalLongs,
        globalDoubles
    );
  }

  private ImmutableBitmap fillBitmap(int... rows)
  {
    MutableBitmap bitmap = roaringFactory.getBitmapFactory().makeEmptyMutableBitmap();
    for (int i : rows) {
      bitmap.add(i);
    }
    return roaringFactory.getBitmapFactory().makeImmutableBitmap(bitmap);
  }

  void checkBitmap(ImmutableBitmap bitmap, int... expectedRows)
  {
    IntIterator iterator = bitmap.iterator();
    for (int i : expectedRows) {
      Assert.assertTrue(iterator.hasNext());
      Assert.assertEquals(i, iterator.next());
    }
    Assert.assertFalse(iterator.hasNext());
  }

  static void writeToBuffer(ByteBuffer buffer, Serializer serializer) throws IOException
  {
    WritableByteChannel channel = new WritableByteChannel()
    {
      @Override
      public int write(ByteBuffer src)
      {
        int size = src.remaining();
        buffer.put(src);
        return size;
      }

      @Override
      public boolean isOpen()
      {
        return true;
      }

      @Override
      public void close()
      {
      }
    };

    serializer.writeTo(channel, null);
    buffer.position(0);
  }
}
