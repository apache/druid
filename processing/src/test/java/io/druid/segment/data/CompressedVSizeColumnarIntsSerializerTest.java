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
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class CompressedVSizeColumnarIntsSerializerTest
{
  private static final int[] MAX_VALUES = new int[]{0xFF, 0xFFFF, 0xFFFFFF, 0x0FFFFFFF};
  private final SegmentWriteOutMedium segmentWriteOutMedium = new OffHeapMemorySegmentWriteOutMedium();
  private final CompressionStrategy compressionStrategy;
  private final ByteOrder byteOrder;
  private final Random rand = new Random(0);
  private int[] vals;
  public CompressedVSizeColumnarIntsSerializerTest(
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

  private void checkSerializedSizeAndData(int chunkSize) throws Exception
  {
    FileSmoosher smoosher = new FileSmoosher(FileUtils.getTempDirectory());

    CompressedVSizeColumnarIntsSerializer writer = new CompressedVSizeColumnarIntsSerializer(
        segmentWriteOutMedium,
        "test",
        vals.length > 0 ? Ints.max(vals) : 0,
        chunkSize,
        byteOrder,
        compressionStrategy
    );
    CompressedVSizeColumnarIntsSupplier supplierFromList = CompressedVSizeColumnarIntsSupplier.fromList(
        IntArrayList.wrap(vals),
        vals.length > 0 ? Ints.max(vals) : 0,
        chunkSize,
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
    CompressedVSizeColumnarIntsSupplier supplierFromByteBuffer = CompressedVSizeColumnarIntsSupplier.fromByteBuffer(
        ByteBuffer.wrap(IOUtils.toByteArray(writeOutBytes.asInputStream())),
        byteOrder
    );
    ColumnarInts columnarInts = supplierFromByteBuffer.get();
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
      final int maxChunkSize = CompressedVSizeColumnarIntsSupplier.maxIntsInBufferForValue(maxValue);
      generateVals(rand.nextInt(maxChunkSize), maxValue);
      checkSerializedSizeAndData(maxChunkSize);
    }
  }

  @Test
  public void testLargeData() throws Exception
  {
    // more than one chunk
    for (int maxValue : MAX_VALUES) {
      final int maxChunkSize = CompressedVSizeColumnarIntsSupplier.maxIntsInBufferForValue(maxValue);
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

  private void checkV2SerializedSizeAndData(int chunkSize) throws Exception
  {
    File tmpDirectory = FileUtils.getTempDirectory();
    FileSmoosher smoosher = new FileSmoosher(tmpDirectory);

    GenericIndexedWriter genericIndexed = GenericIndexedWriter.ofCompressedByteBuffers(
        segmentWriteOutMedium,
        "test",
        compressionStrategy,
        Long.BYTES * 10000
    );
    CompressedVSizeColumnarIntsSerializer writer = new CompressedVSizeColumnarIntsSerializer(
        segmentWriteOutMedium,
        vals.length > 0 ? Ints.max(vals) : 0,
        chunkSize,
        byteOrder,
        compressionStrategy,
        genericIndexed
    );
    writer.open();
    for (int val : vals) {
      writer.addValue(val);
    }

    final SmooshedWriter channel = smoosher.addWithSmooshedWriter(
        "test",
        writer.getSerializedSize()
    );
    writer.writeTo(channel, smoosher);
    channel.close();
    smoosher.close();

    SmooshedFileMapper mapper = Smoosh.map(tmpDirectory);

    CompressedVSizeColumnarIntsSupplier supplierFromByteBuffer = CompressedVSizeColumnarIntsSupplier.fromByteBuffer(
        mapper.mapFile("test"),
        byteOrder
    );

    ColumnarInts columnarInts = supplierFromByteBuffer.get();
    for (int i = 0; i < vals.length; ++i) {
      assertEquals(vals[i], columnarInts.get(i));
    }
    CloseQuietly.close(columnarInts);
    mapper.close();
  }

  @Test
  public void testMultiValueFileLargeData() throws Exception
  {
    for (int maxValue : MAX_VALUES) {
      final int maxChunkSize = CompressedVSizeColumnarIntsSupplier.maxIntsInBufferForValue(maxValue);
      generateVals((rand.nextInt(5) + 5) * maxChunkSize + rand.nextInt(maxChunkSize), maxValue);
      checkV2SerializedSizeAndData(maxChunkSize);
    }
  }

}
