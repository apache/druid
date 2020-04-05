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

package org.apache.druid.segment.data;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.commons.io.IOUtils;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.CloseQuietly;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.io.smoosh.Smoosh;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.java.util.common.io.smoosh.SmooshedWriter;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMedium;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.apache.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import org.apache.druid.segment.writeout.WriteOutBytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

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

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

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
        combinations,
        (Function<List, Object[]>) input -> new Object[]{input.get(0), input.get(1)}
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

  // this test takes ~30 minutes to run
  @Ignore
  @Test
  public void testTooManyValues() throws IOException
  {
    expectedException.expect(ColumnCapacityExceededException.class);
    expectedException.expectMessage(ColumnCapacityExceededException.formatMessage("test"));
    try (
        SegmentWriteOutMedium segmentWriteOutMedium =
            TmpFileSegmentWriteOutMediumFactory.instance().makeSegmentWriteOutMedium(temporaryFolder.newFolder())
    ) {
      CompressedColumnarIntsSerializer serializer = new CompressedColumnarIntsSerializer(
          "test",
          segmentWriteOutMedium,
          "test",
          CompressedColumnarIntsSupplier.MAX_INTS_IN_BUFFER,
          byteOrder,
          compressionStrategy
      );
      serializer.open();

      final long numRows = Integer.MAX_VALUE + 100L;
      for (long i = 0L; i < numRows; i++) {
        serializer.addValue(ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE));
      }
    }
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
    FileSmoosher smoosher = new FileSmoosher(temporaryFolder.newFolder());

    CompressedColumnarIntsSerializer writer = new CompressedColumnarIntsSerializer(
        "test",
        segmentWriteOutMedium,
        "test",
        chunkFactor,
        byteOrder,
        compressionStrategy
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

    Assert.assertEquals(writtenLength, supplierFromList.getSerializedSize());

    // read from ByteBuffer and check values
    CompressedColumnarIntsSupplier supplierFromByteBuffer = CompressedColumnarIntsSupplier.fromByteBuffer(
        ByteBuffer.wrap(IOUtils.toByteArray(writeOutBytes.asInputStream())),
        byteOrder
    );
    ColumnarInts columnarInts = supplierFromByteBuffer.get();
    Assert.assertEquals(vals.length, columnarInts.size());
    for (int i = 0; i < vals.length; ++i) {
      Assert.assertEquals(vals[i], columnarInts.get(i));
    }
    CloseQuietly.close(columnarInts);
  }

  private void checkV2SerializedSizeAndData(int chunkFactor) throws Exception
  {
    File tmpDirectory = FileUtils.createTempDir(StringUtils.format("CompressedIntsIndexedWriterTest_%d", chunkFactor));
    FileSmoosher smoosher = new FileSmoosher(tmpDirectory);

    CompressedColumnarIntsSerializer writer = new CompressedColumnarIntsSerializer(
        "test",
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
    Assert.assertEquals(vals.length, columnarInts.size());
    for (int i = 0; i < vals.length; ++i) {
      Assert.assertEquals(vals[i], columnarInts.get(i));
    }
    CloseQuietly.close(columnarInts);
    mapper.close();
  }
}
