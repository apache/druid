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
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.java.util.common.io.smoosh.Smoosh;
import io.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import io.druid.java.util.common.io.smoosh.SmooshedWriter;
import io.druid.segment.CompressedVSizeIndexedV3Supplier;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class CompressedVSizeIndexedV3WriterTest
{
  private static final int[] OFFSET_CHUNK_FACTORS = new int[]{
      1,
      2,
      100,
      CompressedIntsIndexedSupplier.MAX_INTS_IN_BUFFER
  };
  private static final int[] MAX_VALUES = new int[]{0xFF, 0xFFFF, 0xFFFFFF, 0x0FFFFFFF};
  private final IOPeon ioPeon = new TmpFileIOPeon();
  private final CompressedObjectStrategy.CompressionStrategy compressionStrategy;
  private final ByteOrder byteOrder;
  private final Random rand = new Random(0);
  private List<int[]> vals;

  public CompressedVSizeIndexedV3WriterTest(
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

  private void generateVals(final int totalSize, final int maxValue) throws IOException
  {
    vals = new ArrayList<>(totalSize);
    for (int i = 0; i < totalSize; ++i) {
      int len = rand.nextInt(2) + 1;
      int[] subVals = new int[len];
      for (int j = 0; j < len; ++j) {
        subVals[j] = rand.nextInt(maxValue);
      }
      vals.add(subVals);
    }
  }

  private void checkSerializedSizeAndData(int offsetChunkFactor, int valueChunkFactor) throws Exception
  {
    FileSmoosher smoosher = new FileSmoosher(FileUtils.getTempDirectory());
    final IndexedMultivalue<IndexedInts> indexedMultivalue;

    try (IOPeon ioPeon = new TmpFileIOPeon()) {
      int maxValue = vals.size() > 0 ? getMaxValue(vals) : 0;
      CompressedIntsIndexedWriter offsetWriter = new CompressedIntsIndexedWriter(
          ioPeon, "offset", offsetChunkFactor, byteOrder, compressionStrategy
      );
      CompressedVSizeIntsIndexedWriter valueWriter = new CompressedVSizeIntsIndexedWriter(
          ioPeon, "value", maxValue, valueChunkFactor, byteOrder, compressionStrategy
      );
      CompressedVSizeIndexedV3Writer writer = new CompressedVSizeIndexedV3Writer(offsetWriter, valueWriter);
      CompressedVSizeIndexedV3Supplier supplierFromIterable = CompressedVSizeIndexedV3Supplier.fromIterable(
          Iterables.transform(
              vals, new Function<int[], IndexedInts>()
              {
                @Nullable
                @Override
                public IndexedInts apply(@Nullable final int[] input)
                {
                  return ArrayBasedIndexedInts.of(input);
                }
              }
          ), offsetChunkFactor, maxValue, byteOrder, compressionStrategy
      );
      writer.open();
      for (int[] val : vals) {
        writer.add(val);
      }
      writer.close();
      long writtenLength = writer.getSerializedSize();
      final WritableByteChannel outputChannel = Channels.newChannel(ioPeon.makeOutputStream("output"));
      writer.writeToChannel(outputChannel, smoosher);
      outputChannel.close();
      smoosher.close();

      assertEquals(writtenLength, supplierFromIterable.getSerializedSize());

      // read from ByteBuffer and check values
      CompressedVSizeIndexedV3Supplier supplierFromByteBuffer = CompressedVSizeIndexedV3Supplier.fromByteBuffer(
          ByteBuffer.wrap(IOUtils.toByteArray(ioPeon.makeInputStream("output"))),
          byteOrder,
          null
      );
      indexedMultivalue = supplierFromByteBuffer.get();
      assertEquals(indexedMultivalue.size(), vals.size());
      for (int i = 0; i < vals.size(); ++i) {
        IndexedInts subVals = indexedMultivalue.get(i);
        assertEquals(subVals.size(), vals.get(i).length);
        for (int j = 0; j < subVals.size(); ++j) {
          assertEquals(subVals.get(j), vals.get(i)[j]);
        }
      }
      CloseQuietly.close(indexedMultivalue);
    }
  }

  int getMaxValue(final List<int[]> vals)
  {
    return Ordering.natural().max(
        Iterables.transform(
            vals, new Function<int[], Integer>()
            {
              @Nullable
              @Override
              public Integer apply(int[] input)
              {
                return input.length > 0 ? Ints.max(input) : 0;
              }
            }
        )
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

  @Test
  public void testSmallData() throws Exception
  {
    // less than one chunk
    for (int offsetChunk : OFFSET_CHUNK_FACTORS) {
      for (int maxValue : MAX_VALUES) {
        final int valueChunk = CompressedVSizeIntsIndexedSupplier.maxIntsInBufferForValue(maxValue);
        generateVals(rand.nextInt(valueChunk), maxValue);
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
        final int valueChunk = CompressedVSizeIntsIndexedSupplier.maxIntsInBufferForValue(maxValue);
        generateVals((rand.nextInt(2) + 1) * valueChunk + rand.nextInt(valueChunk), maxValue);
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

  private void checkV2SerializedSizeAndData(int offsetChunkFactor, int valueChunkFactor) throws Exception
  {
    File tmpDirectory = Files.createTempDirectory(StringUtils.format(
        "CompressedVSizeIndexedV3WriterTest_%d_%d",
        offsetChunkFactor,
        offsetChunkFactor
    )).toFile();
    FileSmoosher smoosher = new FileSmoosher(tmpDirectory);
    int maxValue = vals.size() > 0 ? getMaxValue(vals) : 0;

    try (IOPeon ioPeon = new TmpFileIOPeon()) {
      CompressedIntsIndexedWriter offsetWriter = new CompressedIntsIndexedWriter(
          offsetChunkFactor,
          compressionStrategy,
          new GenericIndexedWriter<>(
              ioPeon, "offset",
              CompressedIntBufferObjectStrategy.getBufferForOrder(
                  byteOrder,
                  compressionStrategy,
                  offsetChunkFactor
              ),
              Longs.BYTES * 250000
          )
      );

      GenericIndexedWriter genericIndexed = new GenericIndexedWriter<>(
          ioPeon,
          "value",
          CompressedByteBufferObjectStrategy.getBufferForOrder(
              byteOrder,
              compressionStrategy,
              valueChunkFactor * VSizeIndexedInts.getNumBytesForMax(maxValue)
              + CompressedVSizeIntsIndexedSupplier.bufferPadding(VSizeIndexedInts.getNumBytesForMax(maxValue))
          ),
          Longs.BYTES * 250000
      );
      CompressedVSizeIntsIndexedWriter valueWriter = new CompressedVSizeIntsIndexedWriter(
          ioPeon,
          "value",
          maxValue,
          valueChunkFactor,
          byteOrder,
          compressionStrategy,
          genericIndexed
      );
      CompressedVSizeIndexedV3Writer writer = new CompressedVSizeIndexedV3Writer(offsetWriter, valueWriter);
      writer.open();
      for (int[] val : vals) {
        writer.add(val);
      }
      writer.close();

      final SmooshedWriter channel = smoosher.addWithSmooshedWriter(
          "test",
          writer.getSerializedSize()
      );
      writer.writeToChannel(channel, smoosher);
      channel.close();
      smoosher.close();
      SmooshedFileMapper mapper = Smoosh.map(tmpDirectory);

      CompressedVSizeIndexedV3Supplier supplierFromByteBuffer = CompressedVSizeIndexedV3Supplier.fromByteBuffer(
          mapper.mapFile("test"),
          byteOrder,
          mapper
      );
      IndexedMultivalue<IndexedInts> indexedMultivalue = supplierFromByteBuffer.get();
      assertEquals(indexedMultivalue.size(), vals.size());
      for (int i = 0; i < vals.size(); ++i) {
        IndexedInts subVals = indexedMultivalue.get(i);
        assertEquals(subVals.size(), vals.get(i).length);
        for (int j = 0; j < subVals.size(); ++j) {
          assertEquals(subVals.get(j), vals.get(i)[j]);
        }
      }
      CloseQuietly.close(indexedMultivalue);
      mapper.close();
    }
  }

  @Test
  public void testMultiValueFileLargeData() throws Exception
  {
    // more than one chunk
    for (int offsetChunk : OFFSET_CHUNK_FACTORS) {
      for (int maxValue : MAX_VALUES) {
        final int valueChunk = CompressedVSizeIntsIndexedSupplier.maxIntsInBufferForValue(maxValue);
        generateVals((rand.nextInt(2) + 1) * valueChunk + rand.nextInt(valueChunk), maxValue);
        checkV2SerializedSizeAndData(offsetChunk, valueChunk);
      }
    }
  }
}
