package com.metamx.druid.index.v1;

import com.google.common.io.OutputSupplier;
import com.metamx.druid.collect.ResourceHolder;
import com.metamx.druid.kv.FlattenedArrayWriter;
import com.metamx.druid.kv.IndexedLongs;
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
        new FlattenedArrayWriter<ResourceHolder<LongBuffer>>(
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
