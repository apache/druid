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
import com.google.common.primitives.Longs;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.java.util.common.io.smoosh.Smoosh;
import io.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import io.druid.java.util.common.io.smoosh.SmooshedWriter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class CompressedIntsIndexedWriterTest
{
  private static final int[] MAX_VALUES = new int[]{0xFF, 0xFFFF, 0xFFFFFF, 0x0FFFFFFF};
  private static final int[] CHUNK_FACTORS = new int[]{1, 2, 100, CompressedIntsIndexedSupplier.MAX_INTS_IN_BUFFER};
  private final IOPeon ioPeon = new TmpFileIOPeon();
  private final CompressedObjectStrategy.CompressionStrategy compressionStrategy;
  private final ByteOrder byteOrder;
  private final Random rand = new Random(0);
  private int[] vals;
  public CompressedIntsIndexedWriterTest(
      CompressedObjectStrategy.CompressionStrategy compressionStrategy,
      ByteOrder byteOrder
  )
  {
    this.compressionStrategy = compressionStrategy;
    this.byteOrder = byteOrder;
  }

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

  @Before
  public void setUp() throws Exception
  {
    vals = null;
  }

  @After
  public void tearDown() throws Exception
  {
    ioPeon.close();
  }

  private void generateVals(final int totalSize, final int maxValue) throws IOException
  {
    vals = new int[totalSize];
    for (int i = 0; i < vals.length; ++i) {
      vals[i] = rand.nextInt(maxValue);
    }
  }

  private void checkSerializedSizeAndData(int chunkFactor) throws Exception
  {
    FileSmoosher smoosher = new FileSmoosher(FileUtils.getTempDirectory());

    CompressedIntsIndexedWriter writer = new CompressedIntsIndexedWriter(
        ioPeon, "test", chunkFactor, byteOrder, compressionStrategy
    );
    CompressedIntsIndexedSupplier supplierFromList = CompressedIntsIndexedSupplier.fromList(
        Ints.asList(vals), chunkFactor, byteOrder, compressionStrategy
    );
    writer.open();
    for (int val : vals) {
      writer.add(val);
    }
    writer.close();
    long writtenLength = writer.getSerializedSize();
    final WritableByteChannel outputChannel = Channels.newChannel(ioPeon.makeOutputStream("output"));
    writer.writeToChannel(outputChannel, smoosher);
    outputChannel.close();
    smoosher.close();

    assertEquals(writtenLength, supplierFromList.getSerializedSize());

    // read from ByteBuffer and check values
    CompressedIntsIndexedSupplier supplierFromByteBuffer = CompressedIntsIndexedSupplier.fromByteBuffer(
        ByteBuffer.wrap(IOUtils.toByteArray(ioPeon.makeInputStream("output"))),
        byteOrder,
        null
    );
    IndexedInts indexedInts = supplierFromByteBuffer.get();
    assertEquals(vals.length, indexedInts.size());
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
      for (int chunkFactor : CHUNK_FACTORS) {
        generateVals(rand.nextInt(chunkFactor), maxValue);
        checkSerializedSizeAndData(chunkFactor);
      }
    }
  }

  @Test
  public void testLargeData() throws Exception
  {
    // more than one chunk
    for (int maxValue : MAX_VALUES) {
      for (int chunkFactor : CHUNK_FACTORS) {
        generateVals((rand.nextInt(5) + 5) * chunkFactor + rand.nextInt(chunkFactor), maxValue);
        checkSerializedSizeAndData(chunkFactor);
      }
    }
  }

  @Test
  public void testWriteEmpty() throws Exception
  {
    vals = new int[0];
    checkSerializedSizeAndData(2);
  }


  private void checkV2SerializedSizeAndData(int chunkFactor) throws Exception
  {
    File tmpDirectory = Files.createTempDirectory(StringUtils.format(
        "CompressedIntsIndexedWriterTest_%d",
        chunkFactor
    )).toFile();

    FileSmoosher smoosher = new FileSmoosher(tmpDirectory);
    final IOPeon ioPeon = new TmpFileIOPeon();
    try {

      CompressedIntsIndexedWriter writer = new CompressedIntsIndexedWriter(
          chunkFactor,
          compressionStrategy,
          new GenericIndexedWriter<>(
              ioPeon, "test",
              CompressedIntBufferObjectStrategy.getBufferForOrder(byteOrder, compressionStrategy,
                                                                  chunkFactor
              ), Longs.BYTES * 10000
          )
      );

      writer.open();
      for (int val : vals) {
        writer.add(val);
      }
      writer.close();
      final SmooshedWriter channel = smoosher.addWithSmooshedWriter(
          "test", writer.getSerializedSize()
      );
      writer.writeToChannel(channel, smoosher);
      channel.close();
      smoosher.close();

      SmooshedFileMapper mapper = Smoosh.map(tmpDirectory);

      // read from ByteBuffer and check values
      CompressedIntsIndexedSupplier supplierFromByteBuffer = CompressedIntsIndexedSupplier.fromByteBuffer(
          mapper.mapFile("test"),
          byteOrder,
          mapper
      );
      IndexedInts indexedInts = supplierFromByteBuffer.get();
      assertEquals(vals.length, indexedInts.size());
      for (int i = 0; i < vals.length; ++i) {
        assertEquals(vals[i], indexedInts.get(i));
      }
      CloseQuietly.close(indexedInts);
      mapper.close();
    }
    finally {
      ioPeon.close();
    }
  }

  @Test
  public void testMultiValueFileLargeData() throws Exception
  {
    // more than one chunk
    for (int maxValue : MAX_VALUES) {
      for (int chunkFactor : CHUNK_FACTORS) {
        generateVals((rand.nextInt(5) + 5) * chunkFactor + rand.nextInt(chunkFactor), maxValue);
        checkV2SerializedSizeAndData(chunkFactor);
      }
    }
  }
}
