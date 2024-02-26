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

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.DefaultBitmapResultFactory;
import org.apache.druid.query.filter.DruidObjectPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.TypeStrategies;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.FixedIndexed;
import org.apache.druid.segment.data.FixedIndexedWriter;
import org.apache.druid.segment.data.FrontCodedIntArrayIndexed;
import org.apache.druid.segment.data.FrontCodedIntArrayIndexedWriter;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.GenericIndexedWriter;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.druid.segment.index.BitmapColumnIndex;
import org.apache.druid.segment.index.semantic.DictionaryEncodedStringValueIndex;
import org.apache.druid.segment.index.semantic.DictionaryEncodedValueIndex;
import org.apache.druid.segment.index.semantic.DruidPredicateIndexes;
import org.apache.druid.segment.index.semantic.LexicographicalRangeIndexes;
import org.apache.druid.segment.index.semantic.NullValueIndex;
import org.apache.druid.segment.index.semantic.NumericRangeIndexes;
import org.apache.druid.segment.index.semantic.SpatialIndex;
import org.apache.druid.segment.index.semantic.StringValueSetIndexes;
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

public class NestedFieldColumnIndexSupplierTest extends InitializedNullHandlingTest
{
  private static final int ROW_COUNT = 10;
  static final ColumnConfig ALWAYS_USE_INDEXES = new ColumnConfig()
  {
    @Override
    public double skipValueRangeIndexScale()
    {
      return 1.0;
    }

    @Override
    public double skipValuePredicateIndexScale()
    {
      return 1.0;
    }
  };
  BitmapSerdeFactory roaringFactory = RoaringBitmapSerdeFactory.getInstance();
  BitmapResultFactory<ImmutableBitmap> bitmapResultFactory = new DefaultBitmapResultFactory(
      roaringFactory.getBitmapFactory()
  );
  Supplier<Indexed<ByteBuffer>> globalStrings;
  Supplier<FixedIndexed<Long>> globalLongs;
  Supplier<FixedIndexed<Double>> globalDoubles;
  Supplier<FrontCodedIntArrayIndexed> globalArrays;


  @Before
  public void setup() throws IOException
  {
    ByteBuffer stringBuffer = ByteBuffer.allocate(1 << 12);
    ByteBuffer longBuffer = ByteBuffer.allocate(1 << 12).order(ByteOrder.nativeOrder());
    ByteBuffer doubleBuffer = ByteBuffer.allocate(1 << 12).order(ByteOrder.nativeOrder());
    ByteBuffer arrayBuffer = ByteBuffer.allocate(1 << 12).order(ByteOrder.nativeOrder());


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
    stringWriter.write("g");
    stringWriter.write("gg");
    stringWriter.write("ggg");
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

    FrontCodedIntArrayIndexedWriter arrayWriter = new FrontCodedIntArrayIndexedWriter(
        new OnHeapMemorySegmentWriteOutMedium(),
        ByteOrder.nativeOrder(),
        4
    );
    arrayWriter.open();
    writeToBuffer(arrayBuffer, arrayWriter);

    GenericIndexed<ByteBuffer> strings = GenericIndexed.read(stringBuffer, GenericIndexed.UTF8_STRATEGY);
    globalStrings = () -> strings.singleThreaded();
    globalLongs = FixedIndexed.read(longBuffer, TypeStrategies.LONG, ByteOrder.nativeOrder(), Long.BYTES);
    globalDoubles = FixedIndexed.read(doubleBuffer, TypeStrategies.DOUBLE, ByteOrder.nativeOrder(), Double.BYTES);
    globalArrays = FrontCodedIntArrayIndexed.read(arrayBuffer, ByteOrder.nativeOrder());
  }

  @Test
  public void testSingleTypeStringColumnValueIndex() throws IOException
  {
    NestedFieldColumnIndexSupplier<?> indexSupplier = makeSingleTypeStringSupplier();

    NullValueIndex nullIndex = indexSupplier.as(NullValueIndex.class);
    Assert.assertNotNull(nullIndex);

    // sanity check to make sure we don't return indexes we don't support
    Assert.assertNull(indexSupplier.as(SpatialIndex.class));

    // 10 rows
    // local: [b, foo, fooo, z]
    // column: [foo, b, fooo, b, z, fooo, z, b, b, foo]

    BitmapColumnIndex columnIndex = nullIndex.get();
    Assert.assertNotNull(columnIndex);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    Assert.assertEquals(0, bitmap.size());
  }

  @Test
  public void testSingleTypeStringColumnValueSetIndex() throws IOException
  {
    NestedFieldColumnIndexSupplier<?> indexSupplier = makeSingleTypeStringSupplier();

    StringValueSetIndexes valueSetIndex = indexSupplier.as(StringValueSetIndexes.class);
    Assert.assertNotNull(valueSetIndex);

    // 10 rows
    // local: [b, foo, fooo, z]
    // column: [foo, b, fooo, b, z, fooo, z, b, b, foo]

    BitmapColumnIndex columnIndex = valueSetIndex.forValue("b");
    Assert.assertNotNull(columnIndex);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 1, 3, 7, 8);

    // non-existent in local column
    columnIndex = valueSetIndex.forValue("fo");
    Assert.assertNotNull(columnIndex);
    bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap);

    // set index
    columnIndex = valueSetIndex.forSortedValues(new TreeSet<>(ImmutableSet.of("b", "fooo", "z")));
    Assert.assertNotNull(columnIndex);
    bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 1, 2, 3, 4, 5, 6, 7, 8);
  }

  @Test
  public void testSingleTypeStringColumnRangeIndex() throws IOException
  {
    NestedFieldColumnIndexSupplier<?> indexSupplier = makeSingleTypeStringSupplier();

    LexicographicalRangeIndexes rangeIndex = indexSupplier.as(LexicographicalRangeIndexes.class);
    Assert.assertNotNull(rangeIndex);

    // 10 rows
    // global: [null, a, b, fo, foo, fooo, g, gg, ggg, z]
    // local: [b, foo, fooo, z]
    // column: [foo, b, fooo, b, z, fooo, z, b, b, foo]

    BitmapColumnIndex forRange = rangeIndex.forRange(null, false, "a", false);
    Assert.assertNotNull(forRange);
    ImmutableBitmap bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap);

    forRange = rangeIndex.forRange(null, true, "a", true);
    Assert.assertNotNull(forRange);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap);

    forRange = rangeIndex.forRange(null, false, "b", true);
    Assert.assertNotNull(forRange);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap);

    forRange = rangeIndex.forRange(null, false, "b", false);
    Assert.assertNotNull(forRange);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 1, 3, 7, 8);


    forRange = rangeIndex.forRange("a", false, "b", true);
    Assert.assertNotNull(forRange);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap);

    forRange = rangeIndex.forRange("a", true, "b", false);
    Assert.assertNotNull(forRange);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 1, 3, 7, 8);

    forRange = rangeIndex.forRange("b", false, "fon", false);
    Assert.assertNotNull(forRange);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 1, 3, 7, 8);

    forRange = rangeIndex.forRange("bb", false, "fon", false);
    Assert.assertNotNull(forRange);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap);

    forRange = rangeIndex.forRange("b", true, "foo", false);
    Assert.assertNotNull(forRange);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 9);

    forRange = rangeIndex.forRange("f", true, "g", true);
    Assert.assertNotNull(forRange);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 2, 5, 9);

    forRange = rangeIndex.forRange(null, false, "g", true);
    Assert.assertNotNull(forRange);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 1, 2, 3, 5, 7, 8, 9);

    forRange = rangeIndex.forRange("f", false, null, true);
    Assert.assertNotNull(forRange);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 2, 4, 5, 6, 9);

    forRange = rangeIndex.forRange("b", true, "fooo", true);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 9);

    forRange = rangeIndex.forRange("b", true, "fooo", false);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 2, 5, 9);

    forRange = rangeIndex.forRange(null, true, "fooo", true);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 1, 3, 7, 8, 9);

    forRange = rangeIndex.forRange("b", true, null, false);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 2, 4, 5, 6, 9);

    forRange = rangeIndex.forRange("b", false, null, true);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

    forRange = rangeIndex.forRange(null, true, "fooo", false);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 1, 2, 3, 5, 7, 8, 9);

    forRange = rangeIndex.forRange(null, true, null, true);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

    forRange = rangeIndex.forRange(null, false, null, false);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

    forRange = rangeIndex.forRange(null, true, "foa", false);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 1, 3, 7, 8);

    forRange = rangeIndex.forRange(null, true, "foooa", false);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 1, 2, 3, 5, 7, 8, 9);

    forRange = rangeIndex.forRange("foooa", true, "ggg", false);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap);

    forRange = rangeIndex.forRange("g", true, "gg", false);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap);

    forRange = rangeIndex.forRange("z", true, "zz", false);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap);

    forRange = rangeIndex.forRange("z", false, "zz", false);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 4, 6);
  }

  @Test
  public void testSingleTypeStringColumnRangeIndexWithPredicate() throws IOException
  {
    NestedFieldColumnIndexSupplier<?> indexSupplier = makeSingleTypeStringSupplier();

    LexicographicalRangeIndexes rangeIndex = indexSupplier.as(LexicographicalRangeIndexes.class);
    Assert.assertNotNull(rangeIndex);

    // 10 rows
    // local: [b, foo, fooo, z]
    // column: [foo, b, fooo, b, z, fooo, z, b, b, foo]

    BitmapColumnIndex forRange = rangeIndex.forRange(
        "f",
        true,
        "g",
        true,
        DruidObjectPredicate.notEqualTo("fooo")
    );
    ImmutableBitmap bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 9);

    forRange = rangeIndex.forRange(
        "f",
        true,
        "g",
        true,
        DruidObjectPredicate.equalTo("fooo")
    );
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 2, 5);

    forRange = rangeIndex.forRange(
        null,
        false,
        "z",
        false,
        DruidObjectPredicate.notEqualTo("fooo")
    );
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 1, 3, 4, 6, 7, 8, 9);

    forRange = rangeIndex.forRange(
        null,
        false,
        "z",
        true,
        DruidObjectPredicate.notEqualTo("fooo")
    );
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 1, 3, 7, 8, 9);

    forRange = rangeIndex.forRange(
        "f",
        true,
        null,
        true,
        DruidObjectPredicate.alwaysTrue()
    );
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 2, 4, 5, 6, 9);
  }

  @Test
  public void testSingleTypeStringColumnPredicateIndex() throws IOException
  {
    NestedFieldColumnIndexSupplier<?> indexSupplier = makeSingleTypeStringSupplier();

    DruidPredicateIndexes predicateIndex = indexSupplier.as(DruidPredicateIndexes.class);
    Assert.assertNotNull(predicateIndex);
    DruidPredicateFactory predicateFactory = new InDimFilter.InFilterDruidPredicateFactory(
        null,
        InDimFilter.ValuesSet.copyOf(ImmutableSet.of("b", "z"))
    );

    // 10 rows
    // local: [b, foo, fooo, z]
    // column: [foo, b, fooo, b, z, fooo, z, b, b, foo]

    BitmapColumnIndex columnIndex = predicateIndex.forPredicate(predicateFactory);
    Assert.assertNotNull(columnIndex);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 1, 3, 4, 6, 7, 8);
  }

  @Test
  public void testSingleTypeStringColumnWithNullValueIndex() throws IOException
  {
    NestedFieldColumnIndexSupplier<?> indexSupplier = makeSingleTypeStringWithNullsSupplier();

    NullValueIndex nullIndex = indexSupplier.as(NullValueIndex.class);
    Assert.assertNotNull(nullIndex);

    // 10 rows
    // local: [null, b, foo, fooo, z]
    // column: [foo, null, fooo, b, z, fooo, z, null, null, foo]

    BitmapColumnIndex columnIndex = nullIndex.get();
    Assert.assertNotNull(columnIndex);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 1, 7, 8);
  }

  @Test
  public void testSingleTypeStringColumnWithNullValueSetIndex() throws IOException
  {
    NestedFieldColumnIndexSupplier<?> indexSupplier = makeSingleTypeStringWithNullsSupplier();

    StringValueSetIndexes valueSetIndex = indexSupplier.as(StringValueSetIndexes.class);
    Assert.assertNotNull(valueSetIndex);

    // 10 rows
    // local: [null, b, foo, fooo, z]
    // column: [foo, null, fooo, b, z, fooo, z, null, null, foo]

    BitmapColumnIndex columnIndex = valueSetIndex.forValue("b");
    Assert.assertNotNull(columnIndex);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 3);

    // non-existent in local column
    columnIndex = valueSetIndex.forValue("fo");
    Assert.assertNotNull(columnIndex);
    bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap);

    // set index
    columnIndex = valueSetIndex.forSortedValues(new TreeSet<>(ImmutableSet.of("b", "fooo", "z")));
    Assert.assertNotNull(columnIndex);
    bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 2, 3, 4, 5, 6);
  }

  @Test
  public void testSingleValueStringWithNullRangeIndex() throws IOException
  {
    NestedFieldColumnIndexSupplier<?> indexSupplier = makeSingleTypeStringWithNullsSupplier();

    LexicographicalRangeIndexes rangeIndex = indexSupplier.as(LexicographicalRangeIndexes.class);
    Assert.assertNotNull(rangeIndex);

    // 10 rows
    // local: [null, b, foo, fooo, z]
    // column: [foo, null, fooo, b, z, fooo, z, null, null, foo]

    BitmapColumnIndex forRange = rangeIndex.forRange("f", true, "g", true);
    Assert.assertNotNull(forRange);

    ImmutableBitmap bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 2, 5, 9);

    forRange = rangeIndex.forRange(null, false, "g", true);
    Assert.assertNotNull(forRange);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 2, 3, 5, 9);

    forRange = rangeIndex.forRange(null, false, "a", true);
    Assert.assertNotNull(forRange);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap);

    forRange = rangeIndex.forRange(null, false, "b", true);
    Assert.assertNotNull(forRange);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap);

    forRange = rangeIndex.forRange(null, false, "b", false);
    Assert.assertNotNull(forRange);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 3);

    forRange = rangeIndex.forRange("f", false, null, true);
    Assert.assertNotNull(forRange);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 2, 4, 5, 6, 9);

    forRange = rangeIndex.forRange("b", true, "fooo", true);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 9);

    forRange = rangeIndex.forRange("b", true, "fooo", false);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 2, 5, 9);

    forRange = rangeIndex.forRange(null, true, "fooo", true);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 3, 9);

    forRange = rangeIndex.forRange("b", true, null, false);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 2, 4, 5, 6, 9);

    forRange = rangeIndex.forRange("b", false, null, true);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 2, 3, 4, 5, 6, 9);

    forRange = rangeIndex.forRange(null, true, "fooo", false);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 2, 3, 5, 9);

    forRange = rangeIndex.forRange(null, true, null, true);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 2, 3, 4, 5, 6, 9);

    forRange = rangeIndex.forRange(null, false, null, false);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 2, 3, 4, 5, 6, 9);
  }

  @Test
  public void testSingleValueStringWithNullPredicateIndex() throws IOException
  {
    NestedFieldColumnIndexSupplier<?> indexSupplier = makeSingleTypeStringWithNullsSupplier();

    DruidPredicateIndexes predicateIndex = indexSupplier.as(DruidPredicateIndexes.class);
    Assert.assertNotNull(predicateIndex);
    DruidPredicateFactory predicateFactory = new InDimFilter.InFilterDruidPredicateFactory(
        null,
        InDimFilter.ValuesSet.copyOf(ImmutableSet.of("b", "z"))
    );

    // 10 rows
    // local: [null, b, foo, fooo, z]
    // column: [foo, null, fooo, b, z, fooo, z, null, null, foo]

    BitmapColumnIndex columnIndex = predicateIndex.forPredicate(predicateFactory);
    Assert.assertNotNull(columnIndex);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 3, 4, 6);
  }

  @Test
  public void testSingleTypeLongColumnValueSetIndex() throws IOException
  {
    NestedFieldColumnIndexSupplier<?> indexSupplier = makeSingleTypeLongSupplier();

    StringValueSetIndexes valueSetIndex = indexSupplier.as(StringValueSetIndexes.class);
    Assert.assertNotNull(valueSetIndex);

    // sanity check to make sure we don't return indexes we don't support
    Assert.assertNull(indexSupplier.as(SpatialIndex.class));

    // 10 rows
    // local: [1, 3, 100, 300]
    // column: [100, 1, 300, 1, 3, 3, 100, 300, 300, 1]

    BitmapColumnIndex columnIndex = valueSetIndex.forValue("1");
    Assert.assertNotNull(columnIndex);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 1, 3, 9);

    // set index
    columnIndex = valueSetIndex.forSortedValues(new TreeSet<>(ImmutableSet.of("1", "300", "700")));
    Assert.assertNotNull(columnIndex);
    bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 1, 2, 3, 7, 8, 9);
  }

  @Test
  public void testSingleTypeLongColumnRangeIndex() throws IOException
  {
    NestedFieldColumnIndexSupplier<?> indexSupplier = makeSingleTypeLongSupplier();

    NumericRangeIndexes rangeIndexes = indexSupplier.as(NumericRangeIndexes.class);
    Assert.assertNotNull(rangeIndexes);

    // 10 rows
    // local: [1, 3, 100, 300]
    // column: [100, 1, 300, 1, 3, 3, 100, 300, 300, 1]

    BitmapColumnIndex forRange = rangeIndexes.forRange(10L, true, 400L, true);
    Assert.assertNotNull(forRange);

    ImmutableBitmap bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 2, 6, 7, 8);

    forRange = rangeIndexes.forRange(1, true, 3, true);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap);

    forRange = rangeIndexes.forRange(1, false, 3, true);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 1, 3, 9);

    forRange = rangeIndexes.forRange(1, false, 3, false);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 1, 3, 4, 5, 9);


    forRange = rangeIndexes.forRange(100L, true, 300L, true);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap);


    forRange = rangeIndexes.forRange(100L, true, 300L, false);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 2, 7, 8);


    forRange = rangeIndexes.forRange(100L, false, 300L, true);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 6);


    forRange = rangeIndexes.forRange(100L, false, 300L, false);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 2, 6, 7, 8);

    forRange = rangeIndexes.forRange(null, true, null, true);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

    forRange = rangeIndexes.forRange(null, false, null, false);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
  }

  @Test
  public void testSingleTypeLongColumnPredicateIndex() throws IOException
  {
    NestedFieldColumnIndexSupplier<?> indexSupplier = makeSingleTypeLongSupplier();

    DruidPredicateIndexes predicateIndex = indexSupplier.as(DruidPredicateIndexes.class);
    Assert.assertNotNull(predicateIndex);
    DruidPredicateFactory predicateFactory = new InDimFilter.InFilterDruidPredicateFactory(
        null,
        InDimFilter.ValuesSet.copyOf(ImmutableSet.of("1", "3"))
    );

    // 10 rows
    // local: [1, 3, 100, 300]
    // column: [100, 1, 300, 1, 3, 3, 100, 300, 300, 1]

    BitmapColumnIndex columnIndex = predicateIndex.forPredicate(predicateFactory);
    Assert.assertNotNull(columnIndex);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 1, 3, 4, 5, 9);
  }

  @Test
  public void testSingleTypeLongColumnWithNullValueIndex() throws IOException
  {
    NestedFieldColumnIndexSupplier<?> indexSupplier = makeSingleTypeLongSupplierWithNull();

    NullValueIndex nullIndex = indexSupplier.as(NullValueIndex.class);
    Assert.assertNotNull(nullIndex);

    // 10 rows
    // local: [null, 1, 3, 100, 300]
    // column: [100, 1, null, 1, 3, null, 100, 300, null, 1]

    BitmapColumnIndex columnIndex = nullIndex.get();
    Assert.assertNotNull(columnIndex);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 2, 5, 8);
  }

  @Test
  public void testSingleTypeLongColumnWithNullValueSetIndex() throws IOException
  {
    NestedFieldColumnIndexSupplier<?> indexSupplier = makeSingleTypeLongSupplierWithNull();

    StringValueSetIndexes valueSetIndex = indexSupplier.as(StringValueSetIndexes.class);
    Assert.assertNotNull(valueSetIndex);

    // 10 rows
    // local: [null, 1, 3, 100, 300]
    // column: [100, 1, null, 1, 3, null, 100, 300, null, 1]

    BitmapColumnIndex columnIndex = valueSetIndex.forValue("3");
    Assert.assertNotNull(columnIndex);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 4);

    // set index
    columnIndex = valueSetIndex.forSortedValues(new TreeSet<>(ImmutableSet.of("1", "3", "300")));
    Assert.assertNotNull(columnIndex);
    bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 1, 3, 4, 7, 9);

    // set index with null
    TreeSet<String> treeSet = new TreeSet<>(Comparators.naturalNullsFirst());
    treeSet.add(null);
    treeSet.add("1");
    treeSet.add("3");
    treeSet.add("300");
    columnIndex = valueSetIndex.forSortedValues(treeSet);
    Assert.assertNotNull(columnIndex);
    bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 1, 2, 3, 4, 5, 7, 8, 9);

    // null value should really use NullValueIndex, but this works for classic reasons
    columnIndex = valueSetIndex.forValue(null);
    Assert.assertNotNull(columnIndex);
    bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 2, 5, 8);
  }

  @Test
  public void testSingleValueLongWithNullRangeIndex() throws IOException
  {
    NestedFieldColumnIndexSupplier<?> indexSupplier = makeSingleTypeLongSupplierWithNull();

    NumericRangeIndexes rangeIndexes = indexSupplier.as(NumericRangeIndexes.class);
    Assert.assertNotNull(rangeIndexes);

    // 10 rows
    // local: [null, 1, 3, 100, 300]
    // column: [100, 1, null, 1, 3, null, 100, 300, null, 1]

    BitmapColumnIndex forRange = rangeIndexes.forRange(100, false, 700, true);
    Assert.assertNotNull(forRange);

    ImmutableBitmap bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 6, 7);

    forRange = rangeIndexes.forRange(100, true, 300, true);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap);

    forRange = rangeIndexes.forRange(100, false, 300, true);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 6);

    forRange = rangeIndexes.forRange(100, true, 300, false);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 7);

    forRange = rangeIndexes.forRange(100, false, 300, false);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 6, 7);

    forRange = rangeIndexes.forRange(null, true, null, true);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 1, 3, 4, 6, 7, 9);

    forRange = rangeIndexes.forRange(null, false, null, false);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 1, 3, 4, 6, 7, 9);

    forRange = rangeIndexes.forRange(null, false, 0, false);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap);

    forRange = rangeIndexes.forRange(null, false, 1, false);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 1, 3, 9);

    forRange = rangeIndexes.forRange(null, false, 1, true);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap);
  }

  @Test
  public void testSingleValueLongWithNullPredicateIndex() throws IOException
  {
    NestedFieldColumnIndexSupplier<?> indexSupplier = makeSingleTypeLongSupplierWithNull();

    DruidPredicateIndexes predicateIndex = indexSupplier.as(DruidPredicateIndexes.class);
    Assert.assertNotNull(predicateIndex);
    DruidPredicateFactory predicateFactory = new InDimFilter.InFilterDruidPredicateFactory(
        null,
        InDimFilter.ValuesSet.copyOf(ImmutableSet.of("3", "100"))
    );

    // 10 rows
    // local: [null, 1, 3, 100, 300]
    // column: [100, 1, null, 1, 3, null, 100, 300, null, 1]

    BitmapColumnIndex columnIndex = predicateIndex.forPredicate(predicateFactory);
    Assert.assertNotNull(columnIndex);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 4, 6);
  }

  @Test
  public void testSingleTypeDoubleColumnValueSetIndex() throws IOException
  {
    NestedFieldColumnIndexSupplier<?> indexSupplier = makeSingleTypeDoubleSupplier();

    StringValueSetIndexes valueSetIndex = indexSupplier.as(StringValueSetIndexes.class);
    Assert.assertNotNull(valueSetIndex);

    // sanity check to make sure we don't return indexes we don't support
    Assert.assertNull(indexSupplier.as(SpatialIndex.class));

    // 10 rows
    // local: [1.1, 1.2, 3.3, 6.6]
    // column: [1.1, 1.1, 1.2, 3.3, 1.2, 6.6, 3.3, 1.2, 1.1, 3.3]

    BitmapColumnIndex columnIndex = valueSetIndex.forValue("1.2");
    Assert.assertNotNull(columnIndex);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 2, 4, 7);

    // set index
    columnIndex = valueSetIndex.forSortedValues(new TreeSet<>(ImmutableSet.of("1.2", "3.3", "6.6")));
    Assert.assertNotNull(columnIndex);
    bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 2, 3, 4, 5, 6, 7, 9);
  }

  @Test
  public void testSingleTypeDoubleColumnRangeIndex() throws IOException
  {
    NestedFieldColumnIndexSupplier<?> indexSupplier = makeSingleTypeDoubleSupplier();

    NumericRangeIndexes rangeIndexes = indexSupplier.as(NumericRangeIndexes.class);
    Assert.assertNotNull(rangeIndexes);

    // 10 rows
    // local: [1.1, 1.2, 3.3, 6.6]
    // column: [1.1, 1.1, 1.2, 3.3, 1.2, 6.6, 3.3, 1.2, 1.1, 3.3]

    BitmapColumnIndex forRange = rangeIndexes.forRange(1.0, true, 5.0, true);
    Assert.assertNotNull(forRange);

    ImmutableBitmap bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 1, 2, 3, 4, 6, 7, 8, 9);

    forRange = rangeIndexes.forRange(1.1, false, 3.3, false);
    Assert.assertNotNull(forRange);

    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 1, 2, 3, 4, 6, 7, 8, 9);

    forRange = rangeIndexes.forRange(1.1, true, 3.3, true);
    Assert.assertNotNull(forRange);

    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 2, 4, 7);

    forRange = rangeIndexes.forRange(null, true, null, true);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

    forRange = rangeIndexes.forRange(null, false, null, false);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

    forRange = rangeIndexes.forRange(1.111, true, 1.19, true);
    Assert.assertNotNull(forRange);

    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap);

    forRange = rangeIndexes.forRange(1.01, true, 1.09, true);
    Assert.assertNotNull(forRange);

    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap);

    forRange = rangeIndexes.forRange(0.05, true, 0.98, true);
    Assert.assertNotNull(forRange);

    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap);

    forRange = rangeIndexes.forRange(0.05, true, 1.1, true);
    Assert.assertNotNull(forRange);

    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap);

    forRange = rangeIndexes.forRange(8.99, true, 10.10, true);
    Assert.assertNotNull(forRange);

    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap);

    forRange = rangeIndexes.forRange(8.99, true, 10.10, true);
    Assert.assertNotNull(forRange);

    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap);

    forRange = rangeIndexes.forRange(10.00, true, 10.10, true);
    Assert.assertNotNull(forRange);

    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap);
  }

  @Test
  public void testSingleTypeDoubleColumnPredicateIndex() throws IOException
  {
    NestedFieldColumnIndexSupplier<?> indexSupplier = makeSingleTypeDoubleSupplier();

    DruidPredicateIndexes predicateIndex = indexSupplier.as(DruidPredicateIndexes.class);
    Assert.assertNotNull(predicateIndex);
    DruidPredicateFactory predicateFactory = new InDimFilter.InFilterDruidPredicateFactory(
        null,
        InDimFilter.ValuesSet.copyOf(ImmutableSet.of("1.2", "3.3", "5.0"))
    );

    // 10 rows
    // local: [1.1, 1.2, 3.3, 6.6]
    // column: [1.1, 1.1, 1.2, 3.3, 1.2, 6.6, 3.3, 1.2, 1.1, 3.3]

    BitmapColumnIndex columnIndex = predicateIndex.forPredicate(predicateFactory);
    Assert.assertNotNull(columnIndex);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 2, 3, 4, 6, 7, 9);
  }

  @Test
  public void testSingleTypeDoubleColumnWithNullValueIndex() throws IOException
  {
    NestedFieldColumnIndexSupplier<?> indexSupplier = makeSingleTypeDoubleSupplierWithNull();

    NullValueIndex nullIndex = indexSupplier.as(NullValueIndex.class);
    Assert.assertNotNull(nullIndex);

    // 10 rows
    // local: [null, 1.1, 1.2, 3.3, 6.6]
    // column: [1.1, null, 1.2, null, 1.2, 6.6, null, 1.2, 1.1, 3.3]

    BitmapColumnIndex columnIndex = nullIndex.get();
    Assert.assertNotNull(columnIndex);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 1, 3, 6);
  }

  @Test
  public void testSingleTypeDoubleColumnWithNullValueSetIndex() throws IOException
  {
    NestedFieldColumnIndexSupplier<?> indexSupplier = makeSingleTypeDoubleSupplierWithNull();

    StringValueSetIndexes valueSetIndex = indexSupplier.as(StringValueSetIndexes.class);
    Assert.assertNotNull(valueSetIndex);

    // 10 rows
    // local: [null, 1.1, 1.2, 3.3, 6.6]
    // column: [1.1, null, 1.2, null, 1.2, 6.6, null, 1.2, 1.1, 3.3]

    BitmapColumnIndex columnIndex = valueSetIndex.forValue("6.6");
    Assert.assertNotNull(columnIndex);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 5);

    // set index
    columnIndex = valueSetIndex.forSortedValues(new TreeSet<>(ImmutableSet.of("1.2", "3.3", "7.7")));
    Assert.assertNotNull(columnIndex);
    bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 2, 4, 7, 9);

    // set index with null
    TreeSet<String> treeSet = new TreeSet<>(Comparators.naturalNullsFirst());
    treeSet.add(null);
    treeSet.add("1.2");
    treeSet.add("3.3");
    treeSet.add("7.7");
    columnIndex = valueSetIndex.forSortedValues(treeSet);
    Assert.assertNotNull(columnIndex);
    bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 1, 2, 3, 4, 6, 7, 9);

    // null value should really use NullValueIndex, but this works for classic reasons
    columnIndex = valueSetIndex.forValue(null);
    Assert.assertNotNull(columnIndex);
    bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 1, 3, 6);
  }

  @Test
  public void testSingleValueDoubleWithNullRangeIndex() throws IOException
  {
    NestedFieldColumnIndexSupplier<?> indexSupplier = makeSingleTypeDoubleSupplierWithNull();

    NumericRangeIndexes rangeIndexes = indexSupplier.as(NumericRangeIndexes.class);
    Assert.assertNotNull(rangeIndexes);

    // 10 rows
    // local: [null, 1.1, 1.2, 3.3, 6.6]
    // column: [1.1, null, 1.2, null, 1.2, 6.6, null, 1.2, 1.1, 3.3]

    BitmapColumnIndex forRange = rangeIndexes.forRange(1.1, false, 5.0, true);
    Assert.assertNotNull(forRange);

    ImmutableBitmap bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 2, 4, 7, 8, 9);

    forRange = rangeIndexes.forRange(null, true, null, true);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 2, 4, 5, 7, 8, 9);

    forRange = rangeIndexes.forRange(null, false, null, false);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 2, 4, 5, 7, 8, 9);

    forRange = rangeIndexes.forRange(null, true, 1.0, true);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap);

    forRange = rangeIndexes.forRange(null, true, 1.1, false);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 8);

    forRange = rangeIndexes.forRange(6.6, false, null, false);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 5);

    forRange = rangeIndexes.forRange(6.6, true, null, false);
    bitmap = forRange.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap);
  }

  @Test
  public void testSingleValueDoubleWithNullPredicateIndex() throws IOException
  {
    NestedFieldColumnIndexSupplier<?> indexSupplier = makeSingleTypeDoubleSupplierWithNull();

    DruidPredicateIndexes predicateIndex = indexSupplier.as(DruidPredicateIndexes.class);
    Assert.assertNotNull(predicateIndex);
    DruidPredicateFactory predicateFactory = new InDimFilter.InFilterDruidPredicateFactory(
        null,
        InDimFilter.ValuesSet.copyOf(ImmutableSet.of("1.2", "3.3"))
    );

    // 10 rows
    // local: [null, 1.1, 1.2, 3.3, 6.6]
    // column: [1.1, null, 1.2, null, 1.2, 6.6, null, 1.2, 1.1, 3.3]

    BitmapColumnIndex columnIndex = predicateIndex.forPredicate(predicateFactory);
    Assert.assertNotNull(columnIndex);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 2, 4, 7, 9);
  }

  @Test
  public void testVariantNullValueIndex() throws IOException
  {
    NestedFieldColumnIndexSupplier<?> indexSupplier = makeVariantSupplierWithNull();

    NullValueIndex nullIndex = indexSupplier.as(NullValueIndex.class);
    Assert.assertNotNull(nullIndex);

    // sanity check to make sure we don't return indexes we don't support
    Assert.assertNull(indexSupplier.as(SpatialIndex.class));

    // 10 rows
    // local: [null, b, z, 1, 300, 1.1, 9.9]
    // column: [1, b, null, 9.9, 300, 1, z, null, 1.1, b]

    BitmapColumnIndex columnIndex = nullIndex.get();
    Assert.assertNotNull(columnIndex);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 2, 7);
  }

  @Test
  public void testVariantValueSetIndex() throws IOException
  {
    NestedFieldColumnIndexSupplier<?> indexSupplier = makeVariantSupplierWithNull();

    StringValueSetIndexes valueSetIndex = indexSupplier.as(StringValueSetIndexes.class);
    Assert.assertNotNull(valueSetIndex);

    // 10 rows
    // local: [null, b, z, 1, 300, 1.1, 9.9]
    // column: [1, b, null, 9.9, 300, 1, z, null, 1.1, b]

    BitmapColumnIndex columnIndex = valueSetIndex.forValue("b");
    Assert.assertNotNull(columnIndex);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 1, 9);

    columnIndex = valueSetIndex.forValue("1");
    Assert.assertNotNull(columnIndex);
    bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 5);

    columnIndex = valueSetIndex.forValue("1.1");
    Assert.assertNotNull(columnIndex);
    bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 8);

    // set index
    columnIndex = valueSetIndex.forSortedValues(new TreeSet<>(ImmutableSet.of("b", "300", "9.9", "1.6")));
    Assert.assertNotNull(columnIndex);
    bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 1, 3, 4, 9);

    // set index with null
    TreeSet<String> treeSet = new TreeSet<>(Comparators.naturalNullsFirst());
    treeSet.add(null);
    treeSet.add("b");
    treeSet.add("300");
    treeSet.add("9.9");
    treeSet.add("1.6");
    columnIndex = valueSetIndex.forSortedValues(treeSet);
    Assert.assertNotNull(columnIndex);
    bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 1, 2, 3, 4, 7, 9);

    // null value should really use NullValueIndex, but this works for classic reasons
    columnIndex = valueSetIndex.forValue(null);
    Assert.assertNotNull(columnIndex);
    bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 2, 7);
  }

  @Test
  public void testVariantRangeIndex() throws IOException
  {
    NestedFieldColumnIndexSupplier<?> indexSupplier = makeVariantSupplierWithNull();

    LexicographicalRangeIndexes rangeIndex = indexSupplier.as(LexicographicalRangeIndexes.class);
    Assert.assertNull(rangeIndex);

    NumericRangeIndexes numericRangeIndexes = indexSupplier.as(NumericRangeIndexes.class);
    Assert.assertNull(numericRangeIndexes);
  }

  @Test
  public void testVariantPredicateIndex() throws IOException
  {
    NestedFieldColumnIndexSupplier<?> indexSupplier = makeVariantSupplierWithNull();

    DruidPredicateIndexes predicateIndex = indexSupplier.as(DruidPredicateIndexes.class);
    Assert.assertNotNull(predicateIndex);
    DruidPredicateFactory predicateFactory = new InDimFilter.InFilterDruidPredicateFactory(
        null,
        InDimFilter.ValuesSet.copyOf(ImmutableSet.of("b", "z", "9.9", "300"))
    );

    // 10 rows
    // local: [null, b, z, 1, 300, 1.1, 9.9]
    // column: [1, b, null, 9.9, 300, 1, z, null, 1.1, b]

    BitmapColumnIndex columnIndex = predicateIndex.forPredicate(predicateFactory);
    Assert.assertNotNull(columnIndex);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 1, 3, 4, 6, 9);
  }

  @Test
  public void testDictionaryEncodedStringValueIndex() throws IOException
  {
    NestedFieldColumnIndexSupplier<?> indexSupplier = makeVariantSupplierWithNull();

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

    Assert.assertEquals(7, lowLevelIndex.getCardinality());
    checkBitmap(lowLevelIndex.getBitmap(0), 2, 7);
    checkBitmap(lowLevelIndex.getBitmap(1), 1, 9);
    checkBitmap(lowLevelIndex.getBitmap(-1));
  }

  @Test
  public void testEnsureNoImproperSelectionFromAdjustedGlobals() throws IOException
  {
    // make sure we only pick matching values, not "matching" values from not validating that
    // globalId actually exists before looking it up in local dictionary
    ByteBuffer localDictionaryBuffer = ByteBuffer.allocate(1 << 10).order(ByteOrder.nativeOrder());
    ByteBuffer bitmapsBuffer = ByteBuffer.allocate(1 << 10);

    ByteBuffer stringBuffer = ByteBuffer.allocate(1 << 10);
    ByteBuffer longBuffer = ByteBuffer.allocate(1 << 10).order(ByteOrder.nativeOrder());
    ByteBuffer doubleBuffer = ByteBuffer.allocate(1 << 10).order(ByteOrder.nativeOrder());

    GenericIndexedWriter<String> stringWriter = new GenericIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        "strings",
        GenericIndexed.STRING_STRATEGY
    );
    stringWriter.open();
    stringWriter.write(null);
    stringWriter.write("1");
    writeToBuffer(stringBuffer, stringWriter);

    FixedIndexedWriter<Long> longWriter = new FixedIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        TypeStrategies.LONG,
        ByteOrder.nativeOrder(),
        Long.BYTES,
        true
    );
    longWriter.open();
    longWriter.write(-2L);
    writeToBuffer(longBuffer, longWriter);

    FixedIndexedWriter<Double> doubleWriter = new FixedIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        TypeStrategies.DOUBLE,
        ByteOrder.nativeOrder(),
        Double.BYTES,
        true
    );
    doubleWriter.open();
    writeToBuffer(doubleBuffer, doubleWriter);

    GenericIndexed<ByteBuffer> strings = GenericIndexed.read(stringBuffer, GenericIndexed.UTF8_STRATEGY);
    Supplier<Indexed<ByteBuffer>> stringIndexed = () -> strings.singleThreaded();
    Supplier<FixedIndexed<Long>> longIndexed = FixedIndexed.read(longBuffer, TypeStrategies.LONG, ByteOrder.nativeOrder(), Long.BYTES);
    Supplier<FixedIndexed<Double>> doubleIndexed = FixedIndexed.read(doubleBuffer, TypeStrategies.DOUBLE, ByteOrder.nativeOrder(), Double.BYTES);

    FixedIndexedWriter<Integer> localDictionaryWriter = new FixedIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        CompressedNestedDataComplexColumn.INT_TYPE_STRATEGY,
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
    //    [null, '1'],
    //    [-2],
    //    []
    // ]
    // local: [null, '1', -2]
    // column: ['1', null, -2]

    // null
    localDictionaryWriter.write(0);
    bitmapWriter.write(fillBitmap(1));

    // '1'
    localDictionaryWriter.write(1);
    bitmapWriter.write(fillBitmap(0));

    // -2
    localDictionaryWriter.write(2);
    bitmapWriter.write(fillBitmap(2));

    writeToBuffer(localDictionaryBuffer, localDictionaryWriter);
    writeToBuffer(bitmapsBuffer, bitmapWriter);

    Supplier<FixedIndexed<Integer>> dictionarySupplier = FixedIndexed.read(
        localDictionaryBuffer,
        CompressedNestedDataComplexColumn.INT_TYPE_STRATEGY,
        ByteOrder.nativeOrder(),
        Integer.BYTES
    );

    GenericIndexed<ImmutableBitmap> bitmaps = GenericIndexed.read(bitmapsBuffer, roaringFactory.getObjectStrategy());

    NestedFieldColumnIndexSupplier<?> indexSupplier = new NestedFieldColumnIndexSupplier<>(
        new FieldTypeInfo.TypeSet(
            new FieldTypeInfo.MutableTypeSet().add(ColumnType.STRING)
                                              .add(ColumnType.LONG)
                                              .getByteValue()
        ),
        roaringFactory.getBitmapFactory(),
        ALWAYS_USE_INDEXES,
        bitmaps,
        dictionarySupplier,
        stringIndexed,
        longIndexed,
        doubleIndexed,
        globalArrays,
        null,
        null,
        ROW_COUNT
    );

    StringValueSetIndexes valueSetIndex = indexSupplier.as(StringValueSetIndexes.class);
    Assert.assertNotNull(valueSetIndex);

    // 3 rows
    // local: [null, '1', -2]
    // column: ['1', null, -2]

    BitmapColumnIndex columnIndex = valueSetIndex.forValue("1");
    Assert.assertNotNull(columnIndex);
    ImmutableBitmap bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0);

    columnIndex = valueSetIndex.forValue("-2");
    Assert.assertNotNull(columnIndex);
    bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 2);

    columnIndex = valueSetIndex.forValue("2");
    Assert.assertNotNull(columnIndex);
    bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap);
  }

  @Test
  public void testSkipIndexThresholds() throws IOException
  {
    ColumnConfig twentyPercent = new ColumnConfig()
    {
      @Override
      public double skipValueRangeIndexScale()
      {
        return 0.2;
      }

      @Override
      public double skipValuePredicateIndexScale()
      {
        return 0.2;
      }
    };
    NestedFieldColumnIndexSupplier<?> singleTypeStringSupplier = makeSingleTypeStringSupplier(twentyPercent);
    NestedFieldColumnIndexSupplier<?> singleTypeLongSupplier = makeSingleTypeLongSupplier(twentyPercent);
    NestedFieldColumnIndexSupplier<?> singleTypeDoubleSupplier = makeSingleTypeDoubleSupplier(twentyPercent);
    NestedFieldColumnIndexSupplier<?> variantSupplierWithNull = makeVariantSupplierWithNull(twentyPercent);

    // value cardinality of all of these dictionaries is bigger than the skip threshold, so predicate index short
    // circuit early and return nothing
    DruidPredicateFactory predicateFactory = new InDimFilter.InFilterDruidPredicateFactory(
        null,
        InDimFilter.ValuesSet.copyOf(ImmutableSet.of("0"))
    );
    Assert.assertNull(singleTypeStringSupplier.as(DruidPredicateIndexes.class).forPredicate(predicateFactory));
    Assert.assertNull(singleTypeLongSupplier.as(DruidPredicateIndexes.class).forPredicate(predicateFactory));
    Assert.assertNull(singleTypeDoubleSupplier.as(DruidPredicateIndexes.class).forPredicate(predicateFactory));
    Assert.assertNull(variantSupplierWithNull.as(DruidPredicateIndexes.class).forPredicate(predicateFactory));

    // range index computation is a bit more complicated and done inside of the index maker gizmo because we don't know
    // the range up front
    LexicographicalRangeIndexes stringRange = singleTypeStringSupplier.as(LexicographicalRangeIndexes.class);
    NumericRangeIndexes longRanges = singleTypeLongSupplier.as(NumericRangeIndexes.class);
    NumericRangeIndexes doubleRanges = singleTypeDoubleSupplier.as(NumericRangeIndexes.class);

    // string: [b, foo, fooo, z]
    // small enough should be cool
    Assert.assertNotNull(stringRange.forRange("fo", false, "fooo", false));
    Assert.assertNotNull(stringRange.forRange("fo", false, "fooo", false, DruidObjectPredicate.alwaysTrue()));
    // range too big, no index
    Assert.assertNull(stringRange.forRange("fo", false, "z", false));
    Assert.assertNull(stringRange.forRange("fo", false, "z", false, DruidObjectPredicate.alwaysTrue()));

    // long: [1, 3, 100, 300]
    // small enough should be cool
    Assert.assertNotNull(longRanges.forRange(1, false, 100, true));
    // range too big, no index
    Assert.assertNull(longRanges.forRange(1, false, null, false));

    // double: [1.1, 1.2, 3.3, 6.6]
    // small enough should be cool
    Assert.assertNotNull(doubleRanges.forRange(null, false, 1.2, false));
    // range too big, no index
    Assert.assertNull(doubleRanges.forRange(null, false, 3.3, false));

    // other index types should not be impacted
    Assert.assertNotNull(singleTypeStringSupplier.as(DictionaryEncodedStringValueIndex.class));
    Assert.assertNotNull(singleTypeStringSupplier.as(DictionaryEncodedValueIndex.class));
    Assert.assertNotNull(singleTypeStringSupplier.as(StringValueSetIndexes.class).forValue("foo"));
    Assert.assertNotNull(
        singleTypeStringSupplier.as(StringValueSetIndexes.class)
                                .forSortedValues(new TreeSet<>(ImmutableSet.of("foo", "fooo", "z")))
    );
    Assert.assertNotNull(singleTypeStringSupplier.as(NullValueIndex.class));

    Assert.assertNotNull(singleTypeLongSupplier.as(DictionaryEncodedStringValueIndex.class));
    Assert.assertNotNull(singleTypeLongSupplier.as(DictionaryEncodedValueIndex.class));
    Assert.assertNotNull(singleTypeLongSupplier.as(StringValueSetIndexes.class).forValue("1"));
    Assert.assertNotNull(
        singleTypeLongSupplier.as(StringValueSetIndexes.class)
                              .forSortedValues(new TreeSet<>(ImmutableSet.of("1", "3", "100")))
    );
    Assert.assertNotNull(singleTypeLongSupplier.as(NullValueIndex.class));

    Assert.assertNotNull(singleTypeDoubleSupplier.as(DictionaryEncodedStringValueIndex.class));
    Assert.assertNotNull(singleTypeDoubleSupplier.as(DictionaryEncodedValueIndex.class));
    Assert.assertNotNull(singleTypeDoubleSupplier.as(StringValueSetIndexes.class).forValue("1.1"));
    Assert.assertNotNull(
        singleTypeDoubleSupplier.as(StringValueSetIndexes.class)
                                .forSortedValues(new TreeSet<>(ImmutableSet.of("1.1", "1.2", "3.3")))
    );
    Assert.assertNotNull(singleTypeDoubleSupplier.as(NullValueIndex.class));

    // variant: [null, b, z, 1, 300, 1.1, 9.9]
    Assert.assertNotNull(variantSupplierWithNull.as(DictionaryEncodedStringValueIndex.class));
    Assert.assertNotNull(variantSupplierWithNull.as(DictionaryEncodedValueIndex.class));
    Assert.assertNotNull(variantSupplierWithNull.as(StringValueSetIndexes.class).forValue("b"));
    Assert.assertNotNull(
        variantSupplierWithNull.as(StringValueSetIndexes.class)
                               .forSortedValues(new TreeSet<>(ImmutableSet.of("b", "1", "9.9")))
    );
    Assert.assertNotNull(variantSupplierWithNull.as(NullValueIndex.class));
  }

  private NestedFieldColumnIndexSupplier<?> makeSingleTypeStringSupplier() throws IOException
  {
    return makeSingleTypeStringSupplier(ALWAYS_USE_INDEXES);
  }

  private NestedFieldColumnIndexSupplier<?> makeSingleTypeStringSupplier(ColumnConfig columnConfig) throws IOException
  {
    ByteBuffer localDictionaryBuffer = ByteBuffer.allocate(1 << 12).order(ByteOrder.nativeOrder());
    ByteBuffer bitmapsBuffer = ByteBuffer.allocate(1 << 12);

    FixedIndexedWriter<Integer> localDictionaryWriter = new FixedIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        CompressedNestedDataComplexColumn.INT_TYPE_STRATEGY,
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
    //    [null, a, b, fo, foo, fooo, g, gg, ggg, z],
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
    localDictionaryWriter.write(9);
    bitmapWriter.write(fillBitmap(4, 6));

    writeToBuffer(localDictionaryBuffer, localDictionaryWriter);
    writeToBuffer(bitmapsBuffer, bitmapWriter);

    Supplier<FixedIndexed<Integer>> dictionarySupplier = FixedIndexed.read(
        localDictionaryBuffer,
        CompressedNestedDataComplexColumn.INT_TYPE_STRATEGY,
        ByteOrder.nativeOrder(),
        Integer.BYTES
    );

    GenericIndexed<ImmutableBitmap> bitmaps = GenericIndexed.read(bitmapsBuffer, roaringFactory.getObjectStrategy());

    return new NestedFieldColumnIndexSupplier<>(
        new FieldTypeInfo.TypeSet(
            new FieldTypeInfo.MutableTypeSet().add(ColumnType.STRING).getByteValue()
        ),
        roaringFactory.getBitmapFactory(),
        columnConfig,
        bitmaps,
        dictionarySupplier,
        globalStrings,
        globalLongs,
        globalDoubles,
        globalArrays,
        null,
        null,
        ROW_COUNT
    );
  }

  private NestedFieldColumnIndexSupplier<?> makeSingleTypeStringWithNullsSupplier() throws IOException
  {
    return makeSingleTypeStringWithNullsSupplier(ALWAYS_USE_INDEXES);
  }

  private NestedFieldColumnIndexSupplier<?> makeSingleTypeStringWithNullsSupplier(ColumnConfig columnConfig)
      throws IOException
  {
    ByteBuffer localDictionaryBuffer = ByteBuffer.allocate(1 << 12).order(ByteOrder.nativeOrder());
    ByteBuffer bitmapsBuffer = ByteBuffer.allocate(1 << 12);

    FixedIndexedWriter<Integer> localDictionaryWriter = new FixedIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        CompressedNestedDataComplexColumn.INT_TYPE_STRATEGY,
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
    //    [null, a, b, fo, foo, fooo, g, gg, ggg, z],
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
    localDictionaryWriter.write(9);
    bitmapWriter.write(fillBitmap(4, 6));

    writeToBuffer(localDictionaryBuffer, localDictionaryWriter);
    writeToBuffer(bitmapsBuffer, bitmapWriter);

    Supplier<FixedIndexed<Integer>> dictionarySupplier = FixedIndexed.read(
        localDictionaryBuffer,
        CompressedNestedDataComplexColumn.INT_TYPE_STRATEGY,
        ByteOrder.nativeOrder(),
        Integer.BYTES
    );

    GenericIndexed<ImmutableBitmap> bitmaps = GenericIndexed.read(bitmapsBuffer, roaringFactory.getObjectStrategy());

    return new NestedFieldColumnIndexSupplier<>(
        new FieldTypeInfo.TypeSet(
            new FieldTypeInfo.MutableTypeSet().add(ColumnType.STRING).getByteValue()
        ),
        roaringFactory.getBitmapFactory(),
        columnConfig,
        bitmaps,
        dictionarySupplier,
        globalStrings,
        globalLongs,
        globalDoubles,
        globalArrays,
        null,
        null,
        ROW_COUNT
    );
  }

  private NestedFieldColumnIndexSupplier<?> makeSingleTypeLongSupplier() throws IOException
  {
    return makeSingleTypeLongSupplier(ALWAYS_USE_INDEXES);
  }

  private NestedFieldColumnIndexSupplier<?> makeSingleTypeLongSupplier(ColumnConfig columnConfig) throws IOException
  {
    ByteBuffer localDictionaryBuffer = ByteBuffer.allocate(1 << 12).order(ByteOrder.nativeOrder());
    ByteBuffer bitmapsBuffer = ByteBuffer.allocate(1 << 12);

    FixedIndexedWriter<Integer> localDictionaryWriter = new FixedIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        CompressedNestedDataComplexColumn.INT_TYPE_STRATEGY,
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
    localDictionaryWriter.write(10);
    bitmapWriter.write(fillBitmap(1, 3, 9));

    // 3
    localDictionaryWriter.write(12);
    bitmapWriter.write(fillBitmap(4, 5));

    // 100
    localDictionaryWriter.write(14);
    bitmapWriter.write(fillBitmap(0, 6));

    // 300
    localDictionaryWriter.write(15);
    bitmapWriter.write(fillBitmap(2, 7, 8));

    writeToBuffer(localDictionaryBuffer, localDictionaryWriter);
    writeToBuffer(bitmapsBuffer, bitmapWriter);

    Supplier<FixedIndexed<Integer>> dictionarySupplier = FixedIndexed.read(
        localDictionaryBuffer,
        CompressedNestedDataComplexColumn.INT_TYPE_STRATEGY,
        ByteOrder.nativeOrder(),
        Integer.BYTES
    );

    GenericIndexed<ImmutableBitmap> bitmaps = GenericIndexed.read(bitmapsBuffer, roaringFactory.getObjectStrategy());

    return new NestedFieldColumnIndexSupplier<>(
        new FieldTypeInfo.TypeSet(
            new FieldTypeInfo.MutableTypeSet().add(ColumnType.LONG).getByteValue()
        ),
        roaringFactory.getBitmapFactory(),
        columnConfig,
        bitmaps,
        dictionarySupplier,
        globalStrings,
        globalLongs,
        globalDoubles,
        globalArrays,
        null,
        null,
        ROW_COUNT
    );
  }

  private NestedFieldColumnIndexSupplier<?> makeSingleTypeLongSupplierWithNull() throws IOException
  {
    return makeSingleTypeLongSupplierWithNull(ALWAYS_USE_INDEXES);
  }

  private NestedFieldColumnIndexSupplier<?> makeSingleTypeLongSupplierWithNull(ColumnConfig columnConfig)
      throws IOException
  {
    ByteBuffer localDictionaryBuffer = ByteBuffer.allocate(1 << 12).order(ByteOrder.nativeOrder());
    ByteBuffer bitmapsBuffer = ByteBuffer.allocate(1 << 12);

    FixedIndexedWriter<Integer> localDictionaryWriter = new FixedIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        CompressedNestedDataComplexColumn.INT_TYPE_STRATEGY,
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
    localDictionaryWriter.write(10);
    bitmapWriter.write(fillBitmap(1, 3, 9));

    // 3
    localDictionaryWriter.write(12);
    bitmapWriter.write(fillBitmap(4));

    // 100
    localDictionaryWriter.write(14);
    bitmapWriter.write(fillBitmap(0, 6));

    // 300
    localDictionaryWriter.write(15);
    bitmapWriter.write(fillBitmap(7));

    writeToBuffer(localDictionaryBuffer, localDictionaryWriter);
    writeToBuffer(bitmapsBuffer, bitmapWriter);

    Supplier<FixedIndexed<Integer>> dictionarySupplier = FixedIndexed.read(
        localDictionaryBuffer,
        CompressedNestedDataComplexColumn.INT_TYPE_STRATEGY,
        ByteOrder.nativeOrder(),
        Integer.BYTES
    );

    GenericIndexed<ImmutableBitmap> bitmaps = GenericIndexed.read(bitmapsBuffer, roaringFactory.getObjectStrategy());

    return new NestedFieldColumnIndexSupplier<>(
        new FieldTypeInfo.TypeSet(
            new FieldTypeInfo.MutableTypeSet().add(ColumnType.LONG).getByteValue()
        ),
        roaringFactory.getBitmapFactory(),
        columnConfig,
        bitmaps,
        dictionarySupplier,
        globalStrings,
        globalLongs,
        globalDoubles,
        globalArrays,
        null,
        null,
        ROW_COUNT
    );
  }

  private NestedFieldColumnIndexSupplier<?> makeSingleTypeDoubleSupplier() throws IOException
  {
    return makeSingleTypeDoubleSupplier(ALWAYS_USE_INDEXES);
  }

  private NestedFieldColumnIndexSupplier<?> makeSingleTypeDoubleSupplier(ColumnConfig columnConfig) throws IOException
  {
    ByteBuffer localDictionaryBuffer = ByteBuffer.allocate(1 << 12).order(ByteOrder.nativeOrder());
    ByteBuffer bitmapsBuffer = ByteBuffer.allocate(1 << 12);

    FixedIndexedWriter<Integer> localDictionaryWriter = new FixedIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        CompressedNestedDataComplexColumn.INT_TYPE_STRATEGY,
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
    localDictionaryWriter.write(18);
    bitmapWriter.write(fillBitmap(0, 1, 8));

    // 1.2
    localDictionaryWriter.write(19);
    bitmapWriter.write(fillBitmap(2, 4, 7));

    // 3.3
    localDictionaryWriter.write(22);
    bitmapWriter.write(fillBitmap(3, 6, 9));

    // 6.6
    localDictionaryWriter.write(23);
    bitmapWriter.write(fillBitmap(5));

    writeToBuffer(localDictionaryBuffer, localDictionaryWriter);
    writeToBuffer(bitmapsBuffer, bitmapWriter);

    Supplier<FixedIndexed<Integer>> dictionarySupplier = FixedIndexed.read(
        localDictionaryBuffer,
        CompressedNestedDataComplexColumn.INT_TYPE_STRATEGY,
        ByteOrder.nativeOrder(),
        Integer.BYTES
    );

    GenericIndexed<ImmutableBitmap> bitmaps = GenericIndexed.read(bitmapsBuffer, roaringFactory.getObjectStrategy());

    return new NestedFieldColumnIndexSupplier<>(
        new FieldTypeInfo.TypeSet(
            new FieldTypeInfo.MutableTypeSet().add(ColumnType.DOUBLE).getByteValue()
        ),
        roaringFactory.getBitmapFactory(),
        columnConfig,
        bitmaps,
        dictionarySupplier,
        globalStrings,
        globalLongs,
        globalDoubles,
        globalArrays,
        null,
        null,
        ROW_COUNT
    );
  }

  private NestedFieldColumnIndexSupplier<?> makeSingleTypeDoubleSupplierWithNull() throws IOException
  {
    return makeSingleTypeDoubleSupplierWithNull(ALWAYS_USE_INDEXES);
  }

  private NestedFieldColumnIndexSupplier<?> makeSingleTypeDoubleSupplierWithNull(ColumnConfig columnConfig)
      throws IOException
  {
    ByteBuffer localDictionaryBuffer = ByteBuffer.allocate(1 << 12).order(ByteOrder.nativeOrder());
    ByteBuffer bitmapsBuffer = ByteBuffer.allocate(1 << 12);

    FixedIndexedWriter<Integer> localDictionaryWriter = new FixedIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        CompressedNestedDataComplexColumn.INT_TYPE_STRATEGY,
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
    localDictionaryWriter.write(18);
    bitmapWriter.write(fillBitmap(0, 8));

    // 1.2
    localDictionaryWriter.write(19);
    bitmapWriter.write(fillBitmap(2, 4, 7));

    // 3.3
    localDictionaryWriter.write(22);
    bitmapWriter.write(fillBitmap(9));

    // 6.6
    localDictionaryWriter.write(23);
    bitmapWriter.write(fillBitmap(5));

    writeToBuffer(localDictionaryBuffer, localDictionaryWriter);
    writeToBuffer(bitmapsBuffer, bitmapWriter);

    Supplier<FixedIndexed<Integer>> dictionarySupplier = FixedIndexed.read(
        localDictionaryBuffer,
        CompressedNestedDataComplexColumn.INT_TYPE_STRATEGY,
        ByteOrder.nativeOrder(),
        Integer.BYTES
    );

    GenericIndexed<ImmutableBitmap> bitmaps = GenericIndexed.read(bitmapsBuffer, roaringFactory.getObjectStrategy());

    return new NestedFieldColumnIndexSupplier<>(
        new FieldTypeInfo.TypeSet(
            new FieldTypeInfo.MutableTypeSet().add(ColumnType.DOUBLE).getByteValue()
        ),
        roaringFactory.getBitmapFactory(),
        columnConfig,
        bitmaps,
        dictionarySupplier,
        globalStrings,
        globalLongs,
        globalDoubles,
        globalArrays,
        null,
        null,
        ROW_COUNT
    );
  }

  private NestedFieldColumnIndexSupplier<?> makeVariantSupplierWithNull() throws IOException
  {
    return makeVariantSupplierWithNull(ALWAYS_USE_INDEXES);
  }

  private NestedFieldColumnIndexSupplier<?> makeVariantSupplierWithNull(ColumnConfig columnConfig) throws IOException
  {
    ByteBuffer localDictionaryBuffer = ByteBuffer.allocate(1 << 12).order(ByteOrder.nativeOrder());
    ByteBuffer bitmapsBuffer = ByteBuffer.allocate(1 << 12);

    FixedIndexedWriter<Integer> localDictionaryWriter = new FixedIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        CompressedNestedDataComplexColumn.INT_TYPE_STRATEGY,
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
    localDictionaryWriter.write(9);
    bitmapWriter.write(fillBitmap(6));

    // 1
    localDictionaryWriter.write(10);
    bitmapWriter.write(fillBitmap(0, 5));

    // 300
    localDictionaryWriter.write(15);
    bitmapWriter.write(fillBitmap(4));

    // 1.1
    localDictionaryWriter.write(18);
    bitmapWriter.write(fillBitmap(8));

    // 9.9
    localDictionaryWriter.write(24);
    bitmapWriter.write(fillBitmap(3));

    writeToBuffer(localDictionaryBuffer, localDictionaryWriter);
    writeToBuffer(bitmapsBuffer, bitmapWriter);

    Supplier<FixedIndexed<Integer>> dictionarySupplier = FixedIndexed.read(
        localDictionaryBuffer,
        CompressedNestedDataComplexColumn.INT_TYPE_STRATEGY,
        ByteOrder.nativeOrder(),
        Integer.BYTES
    );

    GenericIndexed<ImmutableBitmap> bitmaps = GenericIndexed.read(bitmapsBuffer, roaringFactory.getObjectStrategy());

    return new NestedFieldColumnIndexSupplier<>(
        new FieldTypeInfo.TypeSet(
            new FieldTypeInfo.MutableTypeSet().add(ColumnType.STRING)
                                              .add(ColumnType.LONG)
                                              .add(ColumnType.DOUBLE)
                                              .getByteValue()
        ),
        roaringFactory.getBitmapFactory(),
        columnConfig,
        bitmaps,
        dictionarySupplier,
        globalStrings,
        globalLongs,
        globalDoubles,
        globalArrays,
        null,
        null,
        ROW_COUNT
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
