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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.stream.IntStream;

@RunWith(Parameterized.class)
public class V3CompressedVSizeColumnarMultiIntsSerializerTest
{
  private static final String TEST_COLUMN_NAME = "test";
  private static final int[] OFFSET_CHUNK_FACTORS = new int[]{
      1,
      2,
      100,
      CompressedColumnarIntsSupplier.MAX_INTS_IN_BUFFER
  };
  private static final int[] MAX_VALUES = new int[]{0xFF, 0xFFFF, 0xFFFFFF, 0x0FFFFFFF};
  private final CompressionStrategy compressionStrategy;
  private final ByteOrder byteOrder;
  private final Random rand = new Random(0);
  private List<int[]> vals;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  public V3CompressedVSizeColumnarMultiIntsSerializerTest(
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

  @Test
  public void testSmallData() throws Exception
  {
    // less than one chunk
    for (int offsetChunk : OFFSET_CHUNK_FACTORS) {
      for (int maxValue : MAX_VALUES) {
        final int valueChunk = CompressedVSizeColumnarIntsSupplier.maxIntsInBufferForValue(maxValue);
        generateVals(rand.nextInt(valueChunk), maxValue, 2);
        checkSerializedSizeAndData(offsetChunk, valueChunk);
      }
    }
  }

  @Test
  public void testLargeData() throws Exception
  {
    // more than one chunk
    for (int offsetChunk : OFFSET_CHUNK_FACTORS) {
      for (int maxValue : MAX_VALUES) {
        final int valueChunk = CompressedVSizeColumnarIntsSupplier.maxIntsInBufferForValue(maxValue);
        generateVals((rand.nextInt(2) + 1) * valueChunk + rand.nextInt(valueChunk), maxValue, 2);
        checkSerializedSizeAndData(offsetChunk, valueChunk);
      }
    }
  }

  @Test
  public void testEmpty() throws Exception
  {
    vals = new ArrayList<>();
    checkSerializedSizeAndData(1, 2);
  }

  @Test
  public void testMultiValueFileLargeData() throws Exception
  {
    // more than one chunk
    for (int offsetChunk : OFFSET_CHUNK_FACTORS) {
      for (int maxValue : MAX_VALUES) {
        final int valueChunk = CompressedVSizeColumnarIntsSupplier.maxIntsInBufferForValue(maxValue);
        generateVals((rand.nextInt(2) + 1) * valueChunk + rand.nextInt(valueChunk), maxValue, 2);
        checkV2SerializedSizeAndData(offsetChunk, valueChunk);
      }
    }
  }

  // this test takes ~30 minutes to run
  @Ignore
  @Test
  public void testTooManyValues() throws Exception
  {
    expectedException.expect(ColumnCapacityExceededException.class);
    expectedException.expectMessage(ColumnCapacityExceededException.formatMessage("test"));
    // more than one chunk
    final int offsetChunk = CompressedColumnarIntsSupplier.MAX_INTS_IN_BUFFER;
    final int maxValue = 0x0FFFFFFF;
    final int valueChunk = CompressedVSizeColumnarIntsSupplier.maxIntsInBufferForValue(maxValue);
    final int numRows = 10000000;
    final int maxValuesPerRow = 1000;
    generateV2SerializedSizeAndData(numRows, maxValue, maxValuesPerRow, offsetChunk, valueChunk);
  }

  private void generateVals(final int rowCount, final int maxValue, final int numValuesPerRow)
  {
    vals = new ArrayList<>(rowCount);
    for (int i = 0; i < rowCount; ++i) {
      vals.add(generateRow(rand, maxValue, numValuesPerRow));
    }
  }

  private int[] generateRow(Random rand, final int maxValue, final int numValuesPerRow)
  {
    int len = rand.nextInt(numValuesPerRow) + 1;
    int[] subVals = new int[len];
    for (int j = 0; j < len; ++j) {
      subVals[j] = rand.nextInt(maxValue);
    }
    return subVals;
  }

  private int getMaxValue(final List<int[]> vals)
  {
    return vals
        .stream()
        .mapToInt(array -> IntStream.of(array).max().orElse(0))
        .max()
        .orElseThrow(NoSuchElementException::new);
  }

  private void checkSerializedSizeAndData(int offsetChunkFactor, int valueChunkFactor) throws Exception
  {
    FileSmoosher smoosher = new FileSmoosher(temporaryFolder.newFolder());

    try (SegmentWriteOutMedium segmentWriteOutMedium = new OffHeapMemorySegmentWriteOutMedium()) {
      int maxValue = vals.size() > 0 ? getMaxValue(vals) : 0;
      CompressedColumnarIntsSerializer offsetWriter = new CompressedColumnarIntsSerializer(
          TEST_COLUMN_NAME,
          segmentWriteOutMedium,
          "offset",
          offsetChunkFactor,
          byteOrder,
          compressionStrategy
      );
      CompressedVSizeColumnarIntsSerializer valueWriter = new CompressedVSizeColumnarIntsSerializer(
          TEST_COLUMN_NAME,
          segmentWriteOutMedium,
          "value",
          maxValue,
          valueChunkFactor,
          byteOrder,
          compressionStrategy
      );
      V3CompressedVSizeColumnarMultiIntsSerializer writer =
          new V3CompressedVSizeColumnarMultiIntsSerializer(TEST_COLUMN_NAME, offsetWriter, valueWriter);
      V3CompressedVSizeColumnarMultiIntsSupplier supplierFromIterable =
          V3CompressedVSizeColumnarMultiIntsSupplier.fromIterable(
              Iterables.transform(vals, ArrayBasedIndexedInts::new),
              offsetChunkFactor,
              maxValue,
              byteOrder,
              compressionStrategy,
              segmentWriteOutMedium.getCloser()
          );
      writer.open();
      for (int[] val : vals) {
        writer.addValues(new ArrayBasedIndexedInts(val));
      }
      long writtenLength = writer.getSerializedSize();
      final WriteOutBytes writeOutBytes = segmentWriteOutMedium.makeWriteOutBytes();
      writer.writeTo(writeOutBytes, smoosher);
      smoosher.close();

      Assert.assertEquals(writtenLength, supplierFromIterable.getSerializedSize());

      // read from ByteBuffer and check values
      V3CompressedVSizeColumnarMultiIntsSupplier supplierFromByteBuffer = V3CompressedVSizeColumnarMultiIntsSupplier.fromByteBuffer(
          ByteBuffer.wrap(IOUtils.toByteArray(writeOutBytes.asInputStream())),
          byteOrder
      );

      try (final ColumnarMultiInts columnarMultiInts = supplierFromByteBuffer.get()) {
        Assert.assertEquals(columnarMultiInts.size(), vals.size());
        for (int i = 0; i < vals.size(); ++i) {
          IndexedInts subVals = columnarMultiInts.get(i);
          Assert.assertEquals(subVals.size(), vals.get(i).length);
          for (int j = 0, size = subVals.size(); j < size; ++j) {
            Assert.assertEquals(subVals.get(j), vals.get(i)[j]);
          }
        }
      }
    }
  }

  private void checkV2SerializedSizeAndData(int offsetChunkFactor, int valueChunkFactor) throws Exception
  {
    File tmpDirectory = FileUtils.createTempDir(StringUtils.format(
        "CompressedVSizeIndexedV3WriterTest_%d_%d",
        offsetChunkFactor,
        offsetChunkFactor
    ));
    FileSmoosher smoosher = new FileSmoosher(tmpDirectory);
    int maxValue = vals.size() > 0 ? getMaxValue(vals) : 0;

    try (SegmentWriteOutMedium segmentWriteOutMedium = new OffHeapMemorySegmentWriteOutMedium()) {
      CompressedColumnarIntsSerializer offsetWriter = new CompressedColumnarIntsSerializer(
          TEST_COLUMN_NAME,
          segmentWriteOutMedium,
          offsetChunkFactor,
          byteOrder,
          compressionStrategy,
          GenericIndexedWriter.ofCompressedByteBuffers(
              segmentWriteOutMedium,
              "offset",
              compressionStrategy,
              Long.BYTES * 250000
          )
      );

      GenericIndexedWriter genericIndexed = GenericIndexedWriter.ofCompressedByteBuffers(
          segmentWriteOutMedium,
          "value",
          compressionStrategy,
          Long.BYTES * 250000
      );
      CompressedVSizeColumnarIntsSerializer valueWriter = new CompressedVSizeColumnarIntsSerializer(
          TEST_COLUMN_NAME,
          segmentWriteOutMedium,
          maxValue,
          valueChunkFactor,
          byteOrder,
          compressionStrategy,
          genericIndexed
      );
      V3CompressedVSizeColumnarMultiIntsSerializer writer =
          new V3CompressedVSizeColumnarMultiIntsSerializer(TEST_COLUMN_NAME, offsetWriter, valueWriter);
      writer.open();
      for (int[] val : vals) {
        writer.addValues(new ArrayBasedIndexedInts(val));
      }

      final SmooshedWriter channel = smoosher.addWithSmooshedWriter("test", writer.getSerializedSize());
      writer.writeTo(channel, smoosher);
      channel.close();
      smoosher.close();
      SmooshedFileMapper mapper = Smoosh.map(tmpDirectory);

      V3CompressedVSizeColumnarMultiIntsSupplier supplierFromByteBuffer =
          V3CompressedVSizeColumnarMultiIntsSupplier.fromByteBuffer(mapper.mapFile("test"), byteOrder);
      ColumnarMultiInts columnarMultiInts = supplierFromByteBuffer.get();
      Assert.assertEquals(columnarMultiInts.size(), vals.size());
      for (int i = 0; i < vals.size(); ++i) {
        IndexedInts subVals = columnarMultiInts.get(i);
        Assert.assertEquals(subVals.size(), vals.get(i).length);
        for (int j = 0, size = subVals.size(); j < size; ++j) {
          Assert.assertEquals(subVals.get(j), vals.get(i)[j]);
        }
      }
      CloseQuietly.close(columnarMultiInts);
      mapper.close();
    }
  }

  private void generateV2SerializedSizeAndData(long numRows, int maxValue, int maxValuesPerRow, int offsetChunkFactor, int valueChunkFactor) throws Exception
  {
    File tmpDirectory = FileUtils.createTempDir(StringUtils.format(
        "CompressedVSizeIndexedV3WriterTest_%d_%d",
        offsetChunkFactor,
        offsetChunkFactor
    ));
    FileSmoosher smoosher = new FileSmoosher(tmpDirectory);

    try (
        SegmentWriteOutMedium segmentWriteOutMedium =
            TmpFileSegmentWriteOutMediumFactory.instance().makeSegmentWriteOutMedium(temporaryFolder.newFolder())
    ) {
      CompressedColumnarIntsSerializer offsetWriter = new CompressedColumnarIntsSerializer(
          TEST_COLUMN_NAME,
          segmentWriteOutMedium,
          offsetChunkFactor,
          byteOrder,
          compressionStrategy,
          GenericIndexedWriter.ofCompressedByteBuffers(
              segmentWriteOutMedium,
              "offset",
              compressionStrategy,
              Long.BYTES * 250000
          )
      );

      GenericIndexedWriter genericIndexed = GenericIndexedWriter.ofCompressedByteBuffers(
          segmentWriteOutMedium,
          "value",
          compressionStrategy,
          Long.BYTES * 250000
      );
      CompressedVSizeColumnarIntsSerializer valueWriter = new CompressedVSizeColumnarIntsSerializer(
          TEST_COLUMN_NAME,
          segmentWriteOutMedium,
          maxValue,
          valueChunkFactor,
          byteOrder,
          compressionStrategy,
          genericIndexed
      );
      V3CompressedVSizeColumnarMultiIntsSerializer writer =
          new V3CompressedVSizeColumnarMultiIntsSerializer(TEST_COLUMN_NAME, offsetWriter, valueWriter);
      writer.open();
      for (long l = 0L; l < numRows; l++) {
        writer.addValues(new ArrayBasedIndexedInts(generateRow(rand, maxValue, maxValuesPerRow)));
      }

      final SmooshedWriter channel = smoosher.addWithSmooshedWriter("test", writer.getSerializedSize());
      writer.writeTo(channel, smoosher);
      channel.close();
      smoosher.close();
      SmooshedFileMapper mapper = Smoosh.map(tmpDirectory);

      V3CompressedVSizeColumnarMultiIntsSupplier supplierFromByteBuffer =
          V3CompressedVSizeColumnarMultiIntsSupplier.fromByteBuffer(mapper.mapFile("test"), byteOrder, mapper);
      ColumnarMultiInts columnarMultiInts = supplierFromByteBuffer.get();
      Assert.assertEquals(columnarMultiInts.size(), numRows);
      Random verifier = new Random(0);
      for (int i = 0; i < numRows; ++i) {
        IndexedInts subVals = columnarMultiInts.get(i);
        int[] expected = generateRow(verifier, maxValue, maxValuesPerRow);
        Assert.assertEquals(subVals.size(), expected.length);
        for (int j = 0, size = subVals.size(); j < size; ++j) {
          Assert.assertEquals(subVals.get(j), expected[j]);
        }
      }
      CloseQuietly.close(columnarMultiInts);
      mapper.close();
    }
  }
}
