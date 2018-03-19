/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.data;

import com.google.common.collect.Iterables;
import io.druid.java.util.common.io.Closer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 */
public class CompressedVSizeColumnarMultiIntsSupplierTest
{
  private Closer closer;
  protected List<int[]> vals;

  protected WritableSupplier<ColumnarMultiInts> columnarMultiIntsSupplier;

  @Before
  public void setUpSimple()
  {
    closer = Closer.create();
    vals = Arrays.asList(
        new int[1],
        new int[]{1, 2, 3, 4, 5},
        new int[]{6, 7, 8, 9, 10},
        new int[]{11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
    );

    columnarMultiIntsSupplier = CompressedVSizeColumnarMultiIntsSupplier.fromIterable(
        Iterables.transform(vals, input -> VSizeColumnarInts.fromArray(input, 20)),
        20,
        ByteOrder.nativeOrder(),
        CompressionStrategy.LZ4,
        closer
    );
  }

  @After
  public void teardown() throws IOException
  {
    columnarMultiIntsSupplier = null;
    vals = null;
    closer.close();
  }

  @Test
  public void testSanity()
  {
    assertSame(vals, columnarMultiIntsSupplier.get());
  }

  @Test
  public void testSerde() throws IOException
  {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    columnarMultiIntsSupplier.writeTo(Channels.newChannel(baos), null);

    final byte[] bytes = baos.toByteArray();
    Assert.assertEquals(columnarMultiIntsSupplier.getSerializedSize(), bytes.length);
    WritableSupplier<ColumnarMultiInts> deserializedColumnarMultiInts = fromByteBuffer(ByteBuffer.wrap(bytes));

    assertSame(vals, deserializedColumnarMultiInts.get());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetInvalidElementInRow()
  {
    columnarMultiIntsSupplier.get().get(3).get(15);
  }

  @Test
  public void testIterators()
  {
    Iterator<IndexedInts> iterator = columnarMultiIntsSupplier.get().iterator();
    int row = 0;
    while (iterator.hasNext()) {
      final int[] ints = vals.get(row);
      final IndexedInts vSizeIndexedInts = iterator.next();

      Assert.assertEquals(ints.length, vSizeIndexedInts.size());
      for (int i = 0, size = vSizeIndexedInts.size(); i < size; i++) {
        Assert.assertEquals(ints[i], vSizeIndexedInts.get(i));
      }
      row++;
    }
  }

  private void assertSame(List<int[]> someInts, ColumnarMultiInts columnarMultiInts)
  {
    Assert.assertEquals(someInts.size(), columnarMultiInts.size());
    for (int i = 0; i < columnarMultiInts.size(); ++i) {
      final int[] ints = someInts.get(i);
      final IndexedInts vSizeIndexedInts = columnarMultiInts.get(i);

      Assert.assertEquals(ints.length, vSizeIndexedInts.size());
      for (int j = 0; j < ints.length; j++) {
        Assert.assertEquals(ints[j], vSizeIndexedInts.get(j));
      }
    }
  }

  protected WritableSupplier<ColumnarMultiInts> fromByteBuffer(ByteBuffer buffer)
  {
    return CompressedVSizeColumnarMultiIntsSupplier.fromByteBuffer(
        buffer,
        ByteOrder.nativeOrder()
    );
  }
}
