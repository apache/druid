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
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.commons.io.IOUtils;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.io.smoosh.Smoosh;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.segment.file.SegmentFileChannel;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMedium;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.apache.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import org.apache.druid.segment.writeout.WriteOutBytes;
import org.apache.druid.utils.CloseableUtils;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@ParameterizedClass
@MethodSource("constructorFeeder")
public class CompressedColumnarIntsSerializerTest
{
  private static final int[] MAX_VALUES = new int[]{0xFF, 0xFFFF, 0xFFFFFF, 0x0FFFFFFF};
  private static final int[] CHUNK_FACTORS = new int[]{1, 2, 100, CompressedColumnarIntsSupplier.MAX_INTS_IN_BUFFER};
  private final SegmentWriteOutMedium segmentWriteOutMedium = new OffHeapMemorySegmentWriteOutMedium();
  @Parameter(0)
  public CompressionStrategy compressionStrategy;
  @Parameter(1)
  public ByteOrder byteOrder;
  private final Random rand = new Random(0);
  private int[] vals;
  @TempDir
  public File temporaryFolder;

  public static Stream<Object[]> constructorFeeder()
  {
    Set<List<Object>> combinations = Sets.cartesianProduct(
        Sets.newHashSet(CompressionStrategy.noNoneValues()),
        Sets.newHashSet(ByteOrder.BIG_ENDIAN, ByteOrder.LITTLE_ENDIAN)
    );

    return StreamSupport.stream(
        Iterables.transform(
            combinations,
            (Function<List, Object[]>) input -> new Object[]{input.get(0), input.get(1)}
        ).spliterator(),
        false
    );
  }

  @BeforeEach
  public void setUp()
  {
    vals = null;
  }

  @AfterEach
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

  @Test
  public void testLargeColumn() throws IOException
  {
    final File columnDir = new File(temporaryFolder, "columnDir");
    FileUtils.mkdirp(columnDir);
    final String columnName = "column";
    final long numRows = 500_000; // enough values that we expect to switch into large-column mode

    try (
        SegmentWriteOutMedium segmentWriteOutMedium =
            TmpFileSegmentWriteOutMediumFactory.instance().makeSegmentWriteOutMedium(new File(temporaryFolder, "medium1"));
        FileSmoosher smoosher = new FileSmoosher(columnDir)
    ) {
      final Random random = new Random(0);
      final int fileSizeLimit = 128_000; // limit to 128KB so we switch to large-column mode sooner
      final CompressedColumnarIntsSerializer serializer = new CompressedColumnarIntsSerializer(
          columnName,
          segmentWriteOutMedium,
          columnName,
          CompressedColumnarIntsSupplier.MAX_INTS_IN_BUFFER,
          byteOrder,
          compressionStrategy,
          fileSizeLimit,
          segmentWriteOutMedium.getCloser()
      );
      serializer.open();

      for (int i = 0; i < numRows; i++) {
        serializer.addValue(random.nextInt() ^ Integer.MIN_VALUE);
      }

      try (SegmentFileChannel primaryWriter = smoosher.addWithChannel(columnName, serializer.getSerializedSize())) {
        serializer.writeTo(primaryWriter, smoosher);
      }
    }

    try (SmooshedFileMapper smooshMapper = SmooshedFileMapper.load(columnDir)) {
      MatcherAssert.assertThat(
          "Number of value parts written", // ensure the column actually ended up multi-part
          smooshMapper.getInternalFilenames().stream().filter(s -> s.startsWith("column_value_")).count(),
          Matchers.greaterThan(1L)
      );

      final Supplier<ColumnarInts> columnSupplier = CompressedColumnarIntsSupplier.fromByteBuffer(
          smooshMapper.mapFile(columnName),
          byteOrder,
          smooshMapper
      );

      try (final ColumnarInts column = columnSupplier.get()) {
        Assertions.assertEquals(numRows, column.size());
      }
    }
  }

  // this test takes ~30 minutes to run
  @Disabled
  @Test
  public void testTooManyValues() throws IOException
  {
    Assertions.assertThrows(
        ColumnCapacityExceededException.class,
        () -> {
          try (
              SegmentWriteOutMedium segmentWriteOutMedium =
                  TmpFileSegmentWriteOutMediumFactory.instance()
                                                     .makeSegmentWriteOutMedium(new File(temporaryFolder, "medium2"))
          ) {
            CompressedColumnarIntsSerializer serializer = new CompressedColumnarIntsSerializer(
                "test",
                segmentWriteOutMedium,
                "test",
                CompressedColumnarIntsSupplier.MAX_INTS_IN_BUFFER,
                byteOrder,
                compressionStrategy,
                GenericIndexedWriter.MAX_FILE_SIZE,
                segmentWriteOutMedium.getCloser()
            );
            serializer.open();

            final long numRows = Integer.MAX_VALUE + 100L;
            for (long i = 0L; i < numRows; i++) {
              serializer.addValue(ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE));
            }
          }
        }
    );
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
    final File smoosherDir = new File(temporaryFolder, StringUtils.replace("smoosher_" + compressionStrategy + "_" + byteOrder.toString(), " ", ""));
    FileUtils.mkdirp(smoosherDir);
    FileSmoosher smoosher = new FileSmoosher(smoosherDir);

    CompressedColumnarIntsSerializer writer = new CompressedColumnarIntsSerializer(
        "test",
        segmentWriteOutMedium,
        "test",
        chunkFactor,
        byteOrder,
        compressionStrategy,
        GenericIndexedWriter.MAX_FILE_SIZE,
        segmentWriteOutMedium.getCloser()
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

    Assertions.assertEquals(writtenLength, supplierFromList.getSerializedSize());

    // read from ByteBuffer and check values
    CompressedColumnarIntsSupplier supplierFromByteBuffer = CompressedColumnarIntsSupplier.fromByteBuffer(
        ByteBuffer.wrap(IOUtils.toByteArray(writeOutBytes.asInputStream())),
        byteOrder,
        null
    );
    ColumnarInts columnarInts = supplierFromByteBuffer.get();
    Assertions.assertEquals(vals.length, columnarInts.size());
    for (int i = 0; i < vals.length; ++i) {
      Assertions.assertEquals(vals[i], columnarInts.get(i));
    }
    CloseableUtils.closeAndWrapExceptions(columnarInts);
    CloseableUtils.closeAndWrapExceptions(segmentWriteOutMedium);
  }

  private void checkV2SerializedSizeAndData(int chunkFactor) throws Exception
  {
    File tmpDirectory = FileUtils.createTempDir(StringUtils.format("CompressedIntsIndexedWriterTest_%d", chunkFactor));
    FileSmoosher smoosher = new FileSmoosher(tmpDirectory);

    CompressedColumnarIntsSerializer writer = new CompressedColumnarIntsSerializer(
        "test",
        chunkFactor,
        byteOrder,
        compressionStrategy,
        GenericIndexedWriter.ofCompressedByteBuffers(
            segmentWriteOutMedium,
            "test",
            compressionStrategy,
            Long.BYTES * 10000,
            GenericIndexedWriter.MAX_FILE_SIZE,
            segmentWriteOutMedium.getCloser()
        ),
        segmentWriteOutMedium.getCloser()
    );

    writer.open();
    writer.addValues(vals, 0, vals.length);
    final SegmentFileChannel channel = smoosher.addWithChannel("test", writer.getSerializedSize());
    writer.writeTo(channel, smoosher);
    channel.close();
    smoosher.close();

    SmooshedFileMapper mapper = Smoosh.map(tmpDirectory);

    // read from ByteBuffer and check values
    CompressedColumnarIntsSupplier supplierFromByteBuffer = CompressedColumnarIntsSupplier.fromByteBuffer(
        mapper.mapFile("test"),
        byteOrder,
        null
    );
    ColumnarInts columnarInts = supplierFromByteBuffer.get();
    Assertions.assertEquals(vals.length, columnarInts.size());
    for (int i = 0; i < vals.length; ++i) {
      Assertions.assertEquals(vals[i], columnarInts.get(i));
    }
    CloseableUtils.closeAndWrapExceptions(columnarInts);
    mapper.close();
  }
}
