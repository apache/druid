package com.metamx.druid.index.v1;

import com.google.common.collect.Maps;
import com.google.common.io.OutputSupplier;
import com.metamx.druid.collect.ResourceHolder;
import com.metamx.druid.kv.FlattenedArrayWriter;
import com.metamx.druid.kv.IOPeon;
import com.metamx.druid.kv.IndexedFloats;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.util.Map;

/**
 */
public class CompressedFloatsSupplierSerializerTest
{
  @Test
  public void testSanity() throws Exception
  {
    final ByteOrder order = ByteOrder.nativeOrder();
    CompressedFloatsSupplierSerializer serializer = new CompressedFloatsSupplierSerializer(
        999,
        new FlattenedArrayWriter<ResourceHolder<FloatBuffer>>(
            new IOPeonForTesting(),
            "test",
            CompressedFloatBufferObjectStrategy.getBufferForOrder(order)
        )
    );
    serializer.open();

    final int numElements = 10000;

    for (int i = 0; i < numElements; ++i) {
      serializer.add((float) i);
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

    IndexedFloats floats = CompressedFloatsIndexedSupplier
        .fromByteBuffer(ByteBuffer.wrap(baos.toByteArray()), order)
        .get();

    Assert.assertEquals(numElements, floats.size());
    for (int i = 0; i < numElements; ++i) {
      Assert.assertEquals((float) i, floats.get(i), 0.0f);
    }

    floats.close();
  }
}
