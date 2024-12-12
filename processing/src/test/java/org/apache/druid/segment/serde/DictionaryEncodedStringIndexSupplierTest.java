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

package org.apache.druid.segment.serde;

import com.google.common.collect.ImmutableSet;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.DefaultBitmapResultFactory;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.GenericIndexedWriter;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.druid.segment.index.BitmapColumnIndex;
import org.apache.druid.segment.index.semantic.StringValueSetIndexes;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMedium;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;
import org.roaringbitmap.IntIterator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;

public class DictionaryEncodedStringIndexSupplierTest extends InitializedNullHandlingTest
{
  BitmapSerdeFactory roaringFactory = RoaringBitmapSerdeFactory.getInstance();
  BitmapResultFactory<ImmutableBitmap> bitmapResultFactory = new DefaultBitmapResultFactory(
      roaringFactory.getBitmapFactory()
  );

  @Test
  public void testStringColumnWithNullValueSetIndex() throws IOException
  {
    StringUtf8ColumnIndexSupplier<?> indexSupplier = makeStringWithNullsSupplier();
    StringValueSetIndexes valueSetIndex = indexSupplier.as(StringValueSetIndexes.class);
    Assert.assertNotNull(valueSetIndex);

    // 10 rows
    // dictionary: [null, b, foo, fooo, z]
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
    columnIndex = valueSetIndex.forSortedValues(InDimFilter.ValuesSet.copyOf(ImmutableSet.of("b", "fooo", "z")));
    Assert.assertNotNull(columnIndex);
    bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 2, 3, 4, 5, 6);

    // set index with single value in middle
    columnIndex = valueSetIndex.forSortedValues(InDimFilter.ValuesSet.copyOf(ImmutableSet.of("foo")));
    Assert.assertNotNull(columnIndex);
    bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap, 0, 9);

    // set index with no element in column and all elements less than lowest non-null value
    columnIndex = valueSetIndex.forSortedValues(InDimFilter.ValuesSet.copyOf(ImmutableSet.of("a", "aa", "aaa")));
    Assert.assertNotNull(columnIndex);
    bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap);

    // set index with no element in column and all elements greater than highest non-null value
    columnIndex = valueSetIndex.forSortedValues(InDimFilter.ValuesSet.copyOf(ImmutableSet.of("zz", "zzz", "zzzz")));
    Assert.assertNotNull(columnIndex);
    bitmap = columnIndex.computeBitmapResult(bitmapResultFactory, false);
    checkBitmap(bitmap);
  }

  private StringUtf8ColumnIndexSupplier<?> makeStringWithNullsSupplier() throws IOException
  {
    ByteBuffer stringBuffer = ByteBuffer.allocate(1 << 12);
    ByteBuffer byteBuffer = ByteBuffer.allocate(1 << 12);

    GenericIndexedWriter<String> stringWriter = new GenericIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        "strings",
        GenericIndexed.STRING_STRATEGY
    );
    GenericIndexedWriter<ByteBuffer> byteBufferWriter = new GenericIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        "byteBuffers",
        GenericIndexed.UTF8_STRATEGY
    );

    stringWriter.open();
    byteBufferWriter.open();

    ByteBuffer bitmapsBuffer = ByteBuffer.allocate(1 << 12);

    GenericIndexedWriter<ImmutableBitmap> bitmapWriter = new GenericIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        "bitmaps",
        roaringFactory.getObjectStrategy()
    );
    bitmapWriter.setObjectsNotSorted();
    bitmapWriter.open();
    // 10 rows
    // dictionary: [null, b, fo, foo, fooo, z]
    // column: [foo, null, fooo, b, z, fooo, z, null, null, foo]

    // null
    stringWriter.write(null);
    byteBufferWriter.write(null);
    bitmapWriter.write(fillBitmap(1, 7, 8));

    // b
    stringWriter.write("b");
    byteBufferWriter.write(ByteBuffer.wrap("b".getBytes(StandardCharsets.UTF_8)));
    bitmapWriter.write(fillBitmap(3));

    // fo
    stringWriter.write("foo");
    byteBufferWriter.write(ByteBuffer.wrap("foo".getBytes(StandardCharsets.UTF_8)));
    bitmapWriter.write(fillBitmap(0, 9));

    // fooo
    stringWriter.write("fooo");
    byteBufferWriter.write(ByteBuffer.wrap("fooo".getBytes(StandardCharsets.UTF_8)));
    bitmapWriter.write(fillBitmap(2, 5));

    // z
    stringWriter.write("z");
    byteBufferWriter.write(ByteBuffer.wrap("z".getBytes(StandardCharsets.UTF_8)));
    bitmapWriter.write(fillBitmap(4, 6));

    writeToBuffer(stringBuffer, stringWriter);
    writeToBuffer(byteBuffer, stringWriter);
    writeToBuffer(bitmapsBuffer, bitmapWriter);

    GenericIndexed<ImmutableBitmap> bitmaps = GenericIndexed.read(bitmapsBuffer, roaringFactory.getObjectStrategy());
    return new StringUtf8ColumnIndexSupplier<>(
        roaringFactory.getBitmapFactory(),
        GenericIndexed.read(byteBuffer, GenericIndexed.UTF8_STRATEGY)::singleThreaded,
        bitmaps,
        null
    );
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

  private ImmutableBitmap fillBitmap(int... rows)
  {
    MutableBitmap bitmap = roaringFactory.getBitmapFactory().makeEmptyMutableBitmap();
    for (int i : rows) {
      bitmap.add(i);
    }
    return roaringFactory.getBitmapFactory().makeImmutableBitmap(bitmap);
  }

  private void checkBitmap(ImmutableBitmap bitmap, int... expectedRows)
  {
    IntIterator iterator = bitmap.iterator();
    for (int i : expectedRows) {
      Assert.assertTrue(iterator.hasNext());
      Assert.assertEquals(i, iterator.next());
    }
    Assert.assertFalse(iterator.hasNext());
  }
}
