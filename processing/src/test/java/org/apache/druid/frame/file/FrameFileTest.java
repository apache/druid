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

package org.apache.druid.frame.file;

import com.google.common.math.IntMath;
import it.unimi.dsi.fastutil.ints.IntObjectPair;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.TestArrayCursorFactory;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.QueryableIndexCursorFactory;
import org.apache.druid.segment.RowAdapters;
import org.apache.druid.segment.RowBasedCursorFactory;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.IncrementalIndexCursorFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.IntStream;

@RunWith(Parameterized.class)
public class FrameFileTest extends InitializedNullHandlingTest
{
  /**
   * Static cache of generated frame files, to speed up tests. Cleared in {@link #afterClass()}.
   */
  private static final Map<FrameFileKey, byte[]> FRAME_FILES = new HashMap<>();

  // Partition every 99 rows if "partitioned" is true.
  private static final int PARTITION_SIZE = 99;

  // Skip unlucky partition #13.
  private static final int SKIP_PARTITION = 13;

  enum AdapterType
  {
    INCREMENTAL {
      @Override
      CursorFactory getCursorFactory()
      {
        return new IncrementalIndexCursorFactory(TestIndex.getNoRollupIncrementalTestIndex());
      }

      @Override
      int getRowCount()
      {
        return TestIndex.getNoRollupIncrementalTestIndex().numRows();
      }
    },
    MMAP {
      @Override
      CursorFactory getCursorFactory()
      {
        return new QueryableIndexCursorFactory(TestIndex.getNoRollupMMappedTestIndex());
      }

      @Override
      int getRowCount()
      {
        return TestIndex.getNoRollupMMappedTestIndex().getNumRows();
      }
    },
    MV_AS_STRING_ARRAYS {
      @Override
      CursorFactory getCursorFactory()
      {
        return new TestArrayCursorFactory(TestIndex.getNoRollupMMappedTestIndex());
      }

      @Override
      int getRowCount()
      {
        return TestIndex.getNoRollupMMappedTestIndex().getNumRows();
      }
    },
    EMPTY {
      @Override
      CursorFactory getCursorFactory()
      {
        return new RowBasedCursorFactory<>(Sequences.empty(), RowAdapters.standardRow(), RowSignature.empty());
      }

      @Override
      int getRowCount()
      {
        return 0;
      }
    };

    abstract CursorFactory getCursorFactory();

    abstract int getRowCount();
  }

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final FrameType frameType;
  private final int maxRowsPerFrame;
  private final boolean partitioned;
  private final AdapterType adapterType;
  private final int maxMmapSize;

  private CursorFactory cursorFactory;
  private int rowCount;
  private File file;

  public FrameFileTest(
      final FrameType frameType,
      final int maxRowsPerFrame,
      final boolean partitioned,
      final AdapterType adapterType,
      final int maxMmapSize
  )
  {
    this.frameType = frameType;
    this.maxRowsPerFrame = maxRowsPerFrame;
    this.partitioned = partitioned;
    this.adapterType = adapterType;
    this.maxMmapSize = maxMmapSize;
  }

  @Parameterized.Parameters(
      name = "frameType = {0}, "
             + "maxRowsPerFrame = {1}, "
             + "partitioned = {2}, "
             + "adapter = {3}, "
             + "maxMmapSize = {4}"
  )
  public static Iterable<Object[]> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();

    for (FrameType frameType : FrameType.values()) {
      for (int maxRowsPerFrame : new int[]{1, 17, 50, PARTITION_SIZE, Integer.MAX_VALUE}) {
        for (boolean partitioned : new boolean[]{true, false}) {
          for (AdapterType adapterType : AdapterType.values()) {
            final int[] maxMmapSizes;

            if (maxRowsPerFrame == 1) {
              maxMmapSizes = new int[]{1_000, 10_000, Integer.MAX_VALUE};
            } else {
              maxMmapSizes = new int[]{Integer.MAX_VALUE};
            }

            for (int maxMmapSize : maxMmapSizes) {
              constructors.add(new Object[]{frameType, maxRowsPerFrame, partitioned, adapterType, maxMmapSize});
            }
          }
        }
      }
    }

    return constructors;
  }

  @Before
  public void setUp() throws IOException
  {
    cursorFactory = adapterType.getCursorFactory();
    rowCount = adapterType.getRowCount();
    file = temporaryFolder.newFile();

    try (final OutputStream out = Files.newOutputStream(file.toPath())) {
      final FrameFileKey frameFileKey = new FrameFileKey(adapterType, frameType, maxRowsPerFrame, partitioned);
      final byte[] frameFileBytes = FRAME_FILES.computeIfAbsent(frameFileKey, FrameFileTest::computeFrameFile);
      out.write(frameFileBytes);
    }
  }

  @AfterClass
  public static void afterClass()
  {
    FRAME_FILES.clear();
  }

  @Test
  public void test_numFrames() throws IOException
  {
    try (final FrameFile frameFile = FrameFile.open(file, maxMmapSize, null)) {
      Assert.assertEquals(computeExpectedNumFrames(), frameFile.numFrames());
    }
  }

  @Test
  public void test_numPartitions() throws IOException
  {
    try (final FrameFile frameFile = FrameFile.open(file, maxMmapSize, null)) {
      Assert.assertEquals(computeExpectedNumPartitions(), frameFile.numPartitions());
    }
  }

  @Test
  public void test_frame_first() throws IOException
  {
    try (final FrameFile frameFile = FrameFile.open(file, maxMmapSize, null)) {
      // Skip test for empty files.
      Assume.assumeThat(frameFile.numFrames(), Matchers.greaterThan(0));

      final Frame firstFrame = frameFile.frame(0);
      Assert.assertEquals(Math.min(rowCount, maxRowsPerFrame), firstFrame.numRows());
    }
  }

  @Test
  public void test_frame_last() throws IOException
  {
    try (final FrameFile frameFile = FrameFile.open(file, maxMmapSize, null)) {
      // Skip test for empty files.
      Assume.assumeThat(frameFile.numFrames(), Matchers.greaterThan(0));

      final Frame lastFrame = frameFile.frame(frameFile.numFrames() - 1);
      Assert.assertEquals(
          rowCount % maxRowsPerFrame != 0
          ? rowCount % maxRowsPerFrame
          : Math.min(rowCount, maxRowsPerFrame),
          lastFrame.numRows()
      );
    }
  }

  @Test
  public void test_frame_outOfBoundsNegative() throws IOException
  {
    try (final FrameFile frameFile = FrameFile.open(file, maxMmapSize, null)) {
      expectedException.expect(IllegalArgumentException.class);
      expectedException.expectMessage("Frame [-1] out of bounds");
      frameFile.frame(-1);
    }
  }

  @Test
  public void test_frame_outOfBoundsTooLarge() throws IOException
  {
    try (final FrameFile frameFile = FrameFile.open(file, maxMmapSize, null)) {
      expectedException.expect(IllegalArgumentException.class);
      expectedException.expectMessage(StringUtils.format("Frame [%,d] out of bounds", frameFile.numFrames()));
      frameFile.frame(frameFile.numFrames());
    }
  }

  @Test
  public void test_frame_readAllDataViaCursorFactory() throws IOException
  {
    final FrameReader frameReader = FrameReader.create(cursorFactory.getRowSignature());

    try (final FrameFile frameFile = FrameFile.open(file, maxMmapSize, null)) {
      final Sequence<List<Object>> frameFileRows = Sequences.concat(
          () -> IntStream.range(0, frameFile.numFrames())
                         .mapToObj(frameFile::frame)
                         .map(frameReader::makeCursorFactory)
                         .map(FrameTestUtil::readRowsFromCursorFactoryWithRowNumber)
                         .iterator()
      );

      final Sequence<List<Object>> adapterRows = FrameTestUtil.readRowsFromCursorFactoryWithRowNumber(cursorFactory);
      FrameTestUtil.assertRowsEqual(adapterRows, frameFileRows);
    }
  }

  @Test
  public void test_getPartitionStartFrame() throws IOException
  {
    try (final FrameFile frameFile = FrameFile.open(file, maxMmapSize, null)) {
      if (partitioned) {
        for (int partitionNum = 0; partitionNum < frameFile.numPartitions(); partitionNum++) {
          Assert.assertEquals(
              "partition #" + partitionNum,
              Math.min(
                  IntMath.divide(
                      (partitionNum >= SKIP_PARTITION ? partitionNum + 1 : partitionNum) * PARTITION_SIZE,
                      maxRowsPerFrame,
                      RoundingMode.CEILING
                  ),
                  frameFile.numFrames()
              ),
              frameFile.getPartitionStartFrame(partitionNum)
          );
        }
      } else {
        Assert.assertEquals(frameFile.numFrames(), frameFile.getPartitionStartFrame(0));
      }
    }
  }

  @Test
  public void test_file() throws IOException
  {
    try (final FrameFile frameFile = FrameFile.open(file, maxMmapSize, null)) {
      Assert.assertEquals(file, frameFile.file());
    }
  }

  @Test
  public void test_open_withDeleteOnClose() throws IOException
  {
    FrameFile.open(file, maxMmapSize, null).close();
    Assert.assertTrue(file.exists());

    FrameFile.open(file, null, FrameFile.Flag.DELETE_ON_CLOSE).close();
    Assert.assertFalse(file.exists());
  }

  @Test
  public void test_newReference() throws IOException
  {
    final FrameFile frameFile1 = FrameFile.open(file, null, FrameFile.Flag.DELETE_ON_CLOSE);
    final FrameFile frameFile2 = frameFile1.newReference();
    final FrameFile frameFile3 = frameFile2.newReference();

    // Closing original file does nothing; must wait for other files to be closed.
    frameFile1.close();
    Assert.assertTrue(file.exists());

    // Can still get a reference after frameFile1 is closed, just because others are still open. Strange but true.
    final FrameFile frameFile4 = frameFile1.newReference();

    // Repeated calls to "close" are deduped.
    frameFile2.close();
    frameFile2.close();
    frameFile2.close();
    frameFile2.close();
    frameFile2.close();
    frameFile2.close();
    Assert.assertTrue(file.exists());

    frameFile3.close();
    Assert.assertTrue(file.exists());

    // Final reference is closed; file is now gone.
    frameFile4.close();
    Assert.assertFalse(file.exists());

    // Can no longer get new references.
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("Frame file is closed");
    frameFile1.newReference();
  }

  private int computeExpectedNumFrames()
  {
    return IntMath.divide(countRows(cursorFactory), maxRowsPerFrame, RoundingMode.CEILING);
  }

  private int computeExpectedNumPartitions()
  {
    if (partitioned) {
      return Math.min(
          computeExpectedNumFrames(),
          IntMath.divide(countRows(cursorFactory), PARTITION_SIZE, RoundingMode.CEILING)
      );
    } else {
      // 0 = not partitioned.
      return 0;
    }
  }

  private static int countRows(final CursorFactory cursorFactory)
  {
    // Not using adapter.getNumRows(), because RowBasedCursorFactory doesn't support it.
    return FrameTestUtil.readRowsFromCursorFactory(cursorFactory, RowSignature.empty(), false)
                        .accumulate(0, (i, in) -> i + 1);
  }

  /**
   * Returns bytes, in frame file format, corresponding to the given {@link FrameFileKey}.
   */
  private static byte[] computeFrameFile(final FrameFileKey frameFileKey)
  {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    try {
      if (frameFileKey.partitioned) {
        // Partition every PARTITION_SIZE rows.
        FrameTestUtil.writeFrameFileWithPartitions(
            FrameSequenceBuilder.fromCursorFactory(frameFileKey.adapterType.getCursorFactory())
                                .frameType(frameFileKey.frameType)
                                .maxRowsPerFrame(frameFileKey.maxRowsPerFrame)
                                .frames()
                                .map(
                                    new Function<>()
                                    {
                                      private int rows = 0;

                                      @Override
                                      public IntObjectPair<Frame> apply(final Frame frame)
                                      {
                                        final int partitionNum = rows / PARTITION_SIZE;
                                        rows += frame.numRows();
                                        return IntObjectPair.of(
                                            partitionNum >= SKIP_PARTITION ? partitionNum + 1 : partitionNum,
                                            frame
                                        );
                                      }
                                    }
                                ),
            baos
        );
      } else {
        FrameTestUtil.writeFrameFile(
            FrameSequenceBuilder.fromCursorFactory(frameFileKey.adapterType.getCursorFactory())
                                .frameType(frameFileKey.frameType)
                                .maxRowsPerFrame(frameFileKey.maxRowsPerFrame)
                                .frames(),
            baos
        );
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    return baos.toByteArray();
  }

  /**
   * Key for {@link #FRAME_FILES}, and input to {@link #computeFrameFile(FrameFileKey)}.
   */
  private static class FrameFileKey
  {
    final AdapterType adapterType;
    final FrameType frameType;
    final int maxRowsPerFrame;
    final boolean partitioned;

    public FrameFileKey(AdapterType adapterType, FrameType frameType, int maxRowsPerFrame, boolean partitioned)
    {
      this.adapterType = adapterType;
      this.frameType = frameType;
      this.maxRowsPerFrame = maxRowsPerFrame;
      this.partitioned = partitioned;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      FrameFileKey that = (FrameFileKey) o;
      return maxRowsPerFrame == that.maxRowsPerFrame
             && partitioned == that.partitioned
             && adapterType == that.adapterType
             && frameType == that.frameType;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(adapterType, frameType, maxRowsPerFrame, partitioned);
    }

    @Override
    public String toString()
    {
      return "FrameFileKey{" +
             "adapterType=" + adapterType +
             ", frameType=" + frameType +
             ", maxRowsPerFrame=" + maxRowsPerFrame +
             ", partitioned=" + partitioned +
             '}';
    }
  }
}
