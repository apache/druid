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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.StringEncodingStrategies;
import org.apache.druid.segment.column.StringEncodingStrategy;
import org.apache.druid.segment.data.DictionaryWriter;
import org.apache.druid.segment.data.FrontCodedIndexed;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMedium;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class NestedDataColumnSupplierFieldDictionaryFixupTest
{
  @Test
  void testFieldDictionaryEmptyPathsFixup() throws IOException
  {
    List<String> values = List.of(
        "$..a",       // bad path, needs fixup
        "$.a",
        "$.b",
        "$.b.[0].a",  // bad path, needs fixup
        "$.b[0].a"
    );
    ByteBuffer buffer = ByteBuffer.allocate(1 << 12);
    buffer.order(ByteOrder.nativeOrder());
    persistToBuffer(buffer, values);

    buffer.position(0);
    Supplier<? extends Indexed<ByteBuffer>> fieldSupplier =
        NestedDataColumnSupplier.getAndFixFieldsSupplier(buffer, ByteOrder.nativeOrder(), null);
    Indexed<ByteBuffer> fieldsDictionary = fieldSupplier.get();
    Assertions.assertInstanceOf(NestedDataColumnSupplier.FieldsFixupIndexed.class, fieldsDictionary);
    // expect get to spit out corrected path values
    Assertions.assertEquals("$[''].a", StringUtils.fromUtf8Nullable(fieldsDictionary.get(0)));
    Assertions.assertEquals("$.a", StringUtils.fromUtf8Nullable(fieldsDictionary.get(1)));
    Assertions.assertEquals("$.b", StringUtils.fromUtf8Nullable(fieldsDictionary.get(2)));
    Assertions.assertEquals("$.b[''][0].a", StringUtils.fromUtf8Nullable(fieldsDictionary.get(3)));
    Assertions.assertEquals("$.b[0].a", StringUtils.fromUtf8Nullable(fieldsDictionary.get(4)));

    // expect to be able to find index of expected correct path instead of bad path
    Assertions.assertEquals(0, fieldsDictionary.indexOf(StringUtils.toUtf8ByteBuffer("$[''].a")));
    Assertions.assertEquals(1, fieldsDictionary.indexOf(StringUtils.toUtf8ByteBuffer("$.a")));
    Assertions.assertEquals(2, fieldsDictionary.indexOf(StringUtils.toUtf8ByteBuffer("$.b")));
    Assertions.assertEquals(3, fieldsDictionary.indexOf(StringUtils.toUtf8ByteBuffer("$.b[''][0].a")));
    Assertions.assertEquals(4, fieldsDictionary.indexOf(StringUtils.toUtf8ByteBuffer("$.b[0].a")));

    List<String> fromIterator = new ArrayList<>();
    for (ByteBuffer byteBuffer : fieldsDictionary) {
      fromIterator.add(StringUtils.fromUtf8Nullable(byteBuffer));
    }
    List<String> expected = List.of(
        "$[''].a",
        "$.a",
        "$.b",
        "$.b[''][0].a",
        "$.b[0].a"
    );
    Assertions.assertEquals(expected, fromIterator);
  }

  private static long persistToBuffer(
      ByteBuffer buffer,
      Iterable<String> sortedIterable
  ) throws IOException
  {
    Iterator<String> sortedStrings = sortedIterable.iterator();
    buffer.position(0);
    OnHeapMemorySegmentWriteOutMedium medium = new OnHeapMemorySegmentWriteOutMedium();
    DictionaryWriter<String> writer = StringEncodingStrategies.getStringDictionaryWriter(
        new StringEncodingStrategy.FrontCoded(4, FrontCodedIndexed.V1),
        medium,
        "test"
    );
    writer.open();
    int index = 0;
    while (sortedStrings.hasNext()) {
      Assertions.assertEquals(index, writer.write(sortedStrings.next()));
      index++;
    }
    Assertions.assertEquals(index, writer.getCardinality());
    Assertions.assertTrue(writer.isSorted());


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
    long size = writer.getSerializedSize();
    buffer.position(0);
    writer.writeTo(channel, null);
    Assertions.assertEquals(size, buffer.position());
    buffer.position(0);
    return size;
  }
}
