/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.segment.data;

import com.google.common.io.OutputSupplier;
import io.druid.collections.ResourceHolder;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;

/**
 */
public class CompressedLongsSupplierSerializerTest
{
  @Test
  public void testSanity() throws Exception
  {
    final ByteOrder order = ByteOrder.nativeOrder();
    CompressedLongsSupplierSerializer serializer = new CompressedLongsSupplierSerializer(
        999,
        new GenericIndexedWriter<ResourceHolder<LongBuffer>>(
            new IOPeonForTesting(),
            "test",
            CompressedLongBufferObjectStrategy.getBufferForOrder(order)
        )
    );
    serializer.open();

    final int numElements = 10000;

    for (int i = 0; i < numElements; ++i) {
      serializer.add((long) i);
    }

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    serializer.closeAndConsolidate(
        new OutputSupplier<OutputStream>()
        {
          @Override
          public OutputStream getOutput() throws IOException
          {
            return baos;
          }
        }
    );

    IndexedLongs longs = CompressedLongsIndexedSupplier
        .fromByteBuffer(ByteBuffer.wrap(baos.toByteArray()), order)
        .get();

    Assert.assertEquals(numElements, longs.size());
    for (int i = 0; i < numElements; ++i) {
      Assert.assertEquals((long) i, longs.get(i), 0.0f);
    }
    longs.close();
  }
}
