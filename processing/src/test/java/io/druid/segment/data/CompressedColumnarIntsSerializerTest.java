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
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.java.util.common.io.smoosh.Smoosh;
import io.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import io.druid.java.util.common.io.smoosh.SmooshedWriter;
import io.druid.segment.writeout.OffHeapMemorySegmentWriteOutMedium;
import io.druid.segment.writeout.SegmentWriteOutMedium;
import io.druid.segment.writeout.WriteOutBytes;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class CompressedColumnarIntsSerializerTest
{
  private static final int[] MAX_VALUES = new int[]{0xFF, 0xFFFF, 0xFFFFFF, 0x0FFFFFFF};
  private static final int[] CHUNK_FACTORS = new int[]{1, 2, 100, CompressedColumnarIntsSupplier.MAX_INTS_IN_BUFFER};
  private final SegmentWriteOutMedium segmentWriteOutMedium = new OffHeapMemorySegmentWriteOutMedium();
  private final CompressionStrategy compressionStrategy;
  private final ByteOrder byteOrder;
  private final Random rand = new Random(0);
  private int[] vals;

  public CompressedColumnarIntsSerializerTest(
      CompressionStrategy compressionStrategy,
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
        Sets.newHashSet(CompressionStrategy.noNoneValues()),
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
  public void setUp()
  {
    vals = null;
  }

  @After
  public void tearDown() throws Exception
  {
    segmentWriteOutMedium.close();
  }

  private void generateVals(final int totalSize, final int maxValue)
  {
    vals = new int[totalSize];
    for (int i = 0; i < vals.length; ++i) {
      vals[i] = rand.nextInt(maxValue);
    }
  }

  private void checkSerializedSizeAndData(int chunkFactor) throws Exception
  {
    FileSmoosher smoosher = new FileSmoosher(FileUtils.getTempDirectory());

    CompressedColumnarIntsSerializer writer = new CompressedColumnarIntsSerializer(
        segmentWriteOutMedium, "test", chunkFactor, byteOrder, compressionStrategy
    );
    CompressedColumnarIntsSupplier supplierFromList = CompressedColumnarIntsSupplier.fromList(
        IntArrayList.wrap(vals),
        chunkFactor,
        byteOrder,
        compressionStrategy,
        segmentWriteOutMedium.getCloser()
    );
    writer.open();
    for (int val : vals) {
      writer.addValue(val);
    }
    long writtenLength = writer.getSerializedSize();
    final WriteOutBytes writeOutBytes = segmentWriteOutMedium.makeWriteOutBytes();
    writer.writeTo(writeOutBytes, smoosher);
    smoosher.close();

    assertEquals(writtenLength, supplierFromList.getSerializedSize());

    // read from ByteBuffer and check values
    CompressedColumnarIntsSupplier supplierFromByteBuffer = CompressedColumnarIntsSupplier.fromByteBuffer(
        ByteBuffer.wrap(IOUtils.toByteArray(writeOutBytes.asInputStream())),
        byteOrder
    );
    ColumnarInts columnarInts = supplierFromByteBuffer.get();
    assertEquals(vals.length, columnarInts.size());
    for (int i = 0; i < vals.length; ++i) {
      assertEquals(vals[i], columnarInts.get(i));
    }
    CloseQuietly.close(columnarInts);
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

    CompressedColumnarIntsSerializer writer = new CompressedColumnarIntsSerializer(
        segmentWriteOutMedium,
        chunkFactor,
        byteOrder,
        compressionStrategy,
        GenericIndexedWriter.ofCompressedByteBuffers(
            segmentWriteOutMedium,
            "test",
            compressionStrategy,
            Long.BYTES * 10000
        )
    );

    writer.open();
    for (int val : vals) {
      writer.addValue(val);
    }
    final SmooshedWriter channel = smoosher.addWithSmooshedWriter("test", writer.getSerializedSize());
    writer.writeTo(channel, smoosher);
    channel.close();
    smoosher.close();

    SmooshedFileMapper mapper = Smoosh.map(tmpDirectory);

    // read from ByteBuffer and check values
    CompressedColumnarIntsSupplier supplierFromByteBuffer = CompressedColumnarIntsSupplier.fromByteBuffer(
        mapper.mapFile("test"),
        byteOrder
    );
    ColumnarInts columnarInts = supplierFromByteBuffer.get();
    assertEquals(vals.length, columnarInts.size());
    for (int i = 0; i < vals.length; ++i) {
      assertEquals(vals[i], columnarInts.get(i));
    }
    CloseQuietly.close(columnarInts);
    mapper.close();
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
