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

import com.google.common.io.OutputSupplier;
import io.druid.collections.ResourceHolder;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;

@RunWith(Parameterized.class)
public class CompressedFloatsSupplierSerializerTest extends CompressionStrategyTest
{
  public CompressedFloatsSupplierSerializerTest(CompressedObjectStrategy.CompressionStrategy compressionStrategy)
  {
    super(compressionStrategy);
  }

  @Test
  public void testSanity() throws Exception
  {
    final ByteOrder order = ByteOrder.nativeOrder();
    final int sizePer = 999;
    CompressedFloatsSupplierSerializer serializer = new CompressedFloatsSupplierSerializer(
        sizePer,
        new GenericIndexedWriter<ResourceHolder<FloatBuffer>>(
            new IOPeonForTesting(),
            "test",
            CompressedFloatBufferObjectStrategy.getBufferForOrder(
                order,
                compressionStrategy,
                sizePer
            )
        ),
        compressionStrategy
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

    Assert.assertEquals(baos.size(), serializer.getSerializedSize());

    IndexedFloats floats = CompressedFloatsIndexedSupplier
        .fromByteBuffer(ByteBuffer.wrap(baos.toByteArray()), order)
        .get();

    Assert.assertEquals(numElements, floats.size());
    for (int i = 0; i < numElements; ++i) {
      Assert.assertEquals((float) i, floats.get(i), 0.0f);
    }

    floats.close();
  }

  @Test
  public void testEmpty() throws Exception
  {
    final ByteOrder order = ByteOrder.nativeOrder();
    final int sizePer = 999;
    CompressedFloatsSupplierSerializer serializer = new CompressedFloatsSupplierSerializer(
        sizePer,
        new GenericIndexedWriter<ResourceHolder<FloatBuffer>>(
            new IOPeonForTesting(),
            "test",
            CompressedFloatBufferObjectStrategy.getBufferForOrder(
                order,
                compressionStrategy,
                sizePer
            )
        ),
        compressionStrategy
    );
    serializer.open();
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

    Assert.assertEquals(baos.size(), serializer.getSerializedSize());
    IndexedFloats floats = CompressedFloatsIndexedSupplier
        .fromByteBuffer(ByteBuffer.wrap(baos.toByteArray()), order)
        .get();

    Assert.assertEquals(0, floats.size());
    floats.close();
  }

}
