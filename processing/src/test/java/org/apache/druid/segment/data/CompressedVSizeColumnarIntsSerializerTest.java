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
import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.commons.io.IOUtils;
import org.apache.druid.java.util.common.FileUtils;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@ParameterizedClass

@MethodSource("constructorFeeder")
public class CompressedVSizeColumnarIntsSerializerTest
{
  private static final int[] MAX_VALUES = new int[]{0xFF, 0xFFFF, 0xFFFFFF, 0x0FFFFFFF};
  private final SegmentWriteOutMedium segmentWriteOutMedium = new OffHeapMemorySegmentWriteOutMedium();
  @Parameter(0)
  public CompressionStrategy compressionStrategy;
  @Parameter(1)
  public ByteOrder byteOrder;
  private final Random rand = new Random(0);
  private int[] vals;
  private final AtomicInteger dirCounter = new AtomicInteger(0);

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

  private void generateVals(final int totalSize, final int maxValue)
  {
    vals = new int[totalSize];
    for (int i = 0; i < vals.length; ++i) {
      vals[i] = rand.nextInt(maxValue);
    }
  }

  private void checkSerializedSizeAndData(int chunkSize) throws Exception
  {
    FileSmoosher smoosher = new FileSmoosher(newSubDir());
    final String columnName = "test";
    CompressedVSizeColumnarIntsSerializer writer = new CompressedVSizeColumnarIntsSerializer(
        columnName,
        segmentWriteOutMedium,
        "test",
        vals.length > 0 ? Ints.max(vals) : 0,
        chunkSize,
        byteOrder,
        compressionStrategy,
        GenericIndexedWriter.MAX_FILE_SIZE,
        segmentWriteOutMedium.getCloser()
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

    Assertions.assertEquals(writtenLength, supplierFromList.getSerializedSize());

    // read from ByteBuffer and check values
    CompressedVSizeColumnarIntsSupplier supplierFromByteBuffer = CompressedVSizeColumnarIntsSupplier.fromByteBuffer(
        ByteBuffer.wrap(IOUtils.toByteArray(writeOutBytes.asInputStream())),
        byteOrder,
        null
    );
    ColumnarInts columnarInts = supplierFromByteBuffer.get();
    for (int i = 0; i < vals.length; ++i) {
      Assertions.assertEquals(vals[i], columnarInts.get(i));
    }
    CloseableUtils.closeAndWrapExceptions(columnarInts);
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


  // this test takes ~18 minutes to run
  @Disabled
  @Test
  public void testTooManyValues()
  {
    final int maxValue = 0x0FFFFFFF;
    final int maxChunkSize = CompressedVSizeColumnarIntsSupplier.maxIntsInBufferForValue(maxValue);
    Assertions.assertThrows(
        ColumnCapacityExceededException.class,
        () -> {
          try (
              SegmentWriteOutMedium segmentWriteOutMedium =
                  TmpFileSegmentWriteOutMediumFactory.instance().makeSegmentWriteOutMedium(newSubDir())
          ) {
            GenericIndexedWriter genericIndexed = GenericIndexedWriter.ofCompressedByteBuffers(
                segmentWriteOutMedium,
                "test",
                compressionStrategy,
                Long.BYTES * 10000,
                GenericIndexedWriter.MAX_FILE_SIZE,
                segmentWriteOutMedium.getCloser()
            );
            CompressedVSizeColumnarIntsSerializer serializer = new CompressedVSizeColumnarIntsSerializer(
                "test",
                maxValue,
                maxChunkSize,
                byteOrder,
                compressionStrategy,
                genericIndexed,
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

  @Test
  public void testEmpty() throws Exception
  {
    vals = new int[0];
    checkSerializedSizeAndData(2);
  }

  private void checkV2SerializedSizeAndData(int chunkSize) throws Exception
  {
    File tmpDirectory = newSubDir();
    FileSmoosher smoosher = new FileSmoosher(tmpDirectory);
    final String columnName = "test";
    GenericIndexedWriter genericIndexed = GenericIndexedWriter.ofCompressedByteBuffers(
        segmentWriteOutMedium,
        "test",
        compressionStrategy,
        Long.BYTES * 10000,
        GenericIndexedWriter.MAX_FILE_SIZE,
        segmentWriteOutMedium.getCloser()
    );
    CompressedVSizeColumnarIntsSerializer writer = new CompressedVSizeColumnarIntsSerializer(
        columnName,
        vals.length > 0 ? Ints.max(vals) : 0,
        chunkSize,
        byteOrder,
        compressionStrategy,
        genericIndexed,
        segmentWriteOutMedium.getCloser()
    );
    writer.open();
    for (int val : vals) {
      writer.addValue(val);
    }

    final SegmentFileChannel channel = smoosher.addWithChannel(
        "test",
        writer.getSerializedSize()
    );
    writer.writeTo(channel, smoosher);
    channel.close();
    smoosher.close();

    SmooshedFileMapper mapper = Smoosh.map(tmpDirectory);

    CompressedVSizeColumnarIntsSupplier supplierFromByteBuffer = CompressedVSizeColumnarIntsSupplier.fromByteBuffer(
        mapper.mapFile("test"),
        byteOrder,
        null
    );

    ColumnarInts columnarInts = supplierFromByteBuffer.get();
    for (int i = 0; i < vals.length; ++i) {
      Assertions.assertEquals(vals[i], columnarInts.get(i));
    }
    CloseableUtils.closeAll(columnarInts, mapper);
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

  @Test
  public void testLargeColumn() throws IOException
  {
    final File columnDir = newSubDir();
    final String columnName = "column";
    final int maxValue = Integer.MAX_VALUE;
    final long numRows = 500_000; // enough values that we expect to switch into large-column mode

    try (
        SegmentWriteOutMedium segmentWriteOutMedium =
            TmpFileSegmentWriteOutMediumFactory.instance().makeSegmentWriteOutMedium(newSubDir());
        FileSmoosher smoosher = new FileSmoosher(columnDir)
    ) {
      final Random random = new Random(0);
      final int fileSizeLimit = 128_000; // limit to 128KB so we switch to large-column mode sooner
      final CompressedVSizeColumnarIntsSerializer serializer = new CompressedVSizeColumnarIntsSerializer(
          columnName,
          segmentWriteOutMedium,
          columnName,
          maxValue,
          CompressedVSizeColumnarIntsSupplier.maxIntsInBufferForValue(maxValue),
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

      final Supplier<ColumnarInts> columnSupplier = CompressedVSizeColumnarIntsSupplier.fromByteBuffer(
          smooshMapper.mapFile(columnName),
          byteOrder,
          smooshMapper
      );

      try (final ColumnarInts column = columnSupplier.get()) {
        Assertions.assertEquals(numRows, column.size());
      }
    }
  }

  private File newSubDir()
  {
    return FileUtils.createTempDirInLocation(temporaryFolder.toPath(), "dir" + dirCounter.getAndIncrement());
  }
}
