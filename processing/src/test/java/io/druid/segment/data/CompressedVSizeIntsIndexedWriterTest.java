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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import io.druid.java.util.common.guava.CloseQuietly;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class CompressedVSizeIntsIndexedWriterTest
{
  @Parameterized.Parameters(name = "{index}: compression={0}, byteOrder={1}")
  public static Iterable<Object[]> compressionStrategiesAndByteOrders()
  {
    Set<List<Object>> combinations = Sets.cartesianProduct(
        Sets.newHashSet(CompressedObjectStrategy.CompressionStrategy.noNoneValues()),
        Sets.newHashSet(ByteOrder.BIG_ENDIAN, ByteOrder.LITTLE_ENDIAN)
    );

    return Iterables.transform(
        combinations, new Function<List, Object[]>()
        {
          @Override
          public Object[] apply(List input)
          {
            return new Object[]{input.get(0), input.get(1)};
          }
        }
    );
  }

  private static final int[] MAX_VALUES = new int[]{0xFF, 0xFFFF, 0xFFFFFF, 0x0FFFFFFF};

  private final IOPeon ioPeon = new TmpFileIOPeon();
  private final CompressedObjectStrategy.CompressionStrategy compressionStrategy;
  private final ByteOrder byteOrder;
  private final Random rand = new Random(0);
  private int[] vals;

  public CompressedVSizeIntsIndexedWriterTest(
      CompressedObjectStrategy.CompressionStrategy compressionStrategy,
      ByteOrder byteOrder
  )
  {
    this.compressionStrategy = compressionStrategy;
    this.byteOrder = byteOrder;
  }

  @Before
  public void setUp() throws Exception
  {
    vals = null;
  }

  @After
  public void tearDown() throws Exception
  {
    ioPeon.cleanup();
  }

  private void generateVals(final int totalSize, final int maxValue) throws IOException
  {
    vals = new int[totalSize];
    for (int i = 0; i < vals.length; ++i) {
      vals[i] = rand.nextInt(maxValue);
    }
  }

  private void checkSerializedSizeAndData(int chunkSize) throws Exception
  {
    CompressedVSizeIntsIndexedWriter writer = new CompressedVSizeIntsIndexedWriter(
        ioPeon, "test", vals.length > 0 ? Ints.max(vals) : 0, chunkSize, byteOrder, compressionStrategy
    );
    CompressedVSizeIntsIndexedSupplier supplierFromList = CompressedVSizeIntsIndexedSupplier.fromList(
        Ints.asList(vals), vals.length > 0 ? Ints.max(vals) : 0, chunkSize, byteOrder, compressionStrategy
    );
    writer.open();
    for (int val : vals) {
      writer.add(val);
    }
    writer.close();
    long writtenLength = writer.getSerializedSize();
    final WritableByteChannel outputChannel = Channels.newChannel(ioPeon.makeOutputStream("output"));
    writer.writeToChannel(outputChannel);
    outputChannel.close();

    assertEquals(writtenLength, supplierFromList.getSerializedSize());

    // read from ByteBuffer and check values
    CompressedVSizeIntsIndexedSupplier supplierFromByteBuffer = CompressedVSizeIntsIndexedSupplier.fromByteBuffer(
        ByteBuffer.wrap(IOUtils.toByteArray(ioPeon.makeInputStream("output"))), byteOrder
    );
    IndexedInts indexedInts = supplierFromByteBuffer.get();
    for (int i = 0; i < vals.length; ++i) {
      assertEquals(vals[i], indexedInts.get(i));
    }
    CloseQuietly.close(indexedInts);
  }

  @Test
  public void testSmallData() throws Exception
  {
    // less than one chunk
    for (int maxValue : MAX_VALUES) {
      final int maxChunkSize = CompressedVSizeIntsIndexedSupplier.maxIntsInBufferForValue(maxValue);
      generateVals(rand.nextInt(maxChunkSize), maxValue);
      checkSerializedSizeAndData(maxChunkSize);
    }
  }

  @Test
  public void testLargeData() throws Exception
  {
    // more than one chunk
    for (int maxValue : MAX_VALUES) {
      final int maxChunkSize = CompressedVSizeIntsIndexedSupplier.maxIntsInBufferForValue(maxValue);
      generateVals((rand.nextInt(5) + 5) * maxChunkSize + rand.nextInt(maxChunkSize), maxValue);
      checkSerializedSizeAndData(maxChunkSize);
    }
  }

  @Test
  public void testEmpty() throws Exception
  {
    vals = new int[0];
    checkSerializedSizeAndData(2);
  }
}
