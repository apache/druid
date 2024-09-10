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

package org.apache.druid.msq.shuffle.output;

import com.google.common.io.ByteStreams;
import com.google.common.math.IntMath;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.channel.BlockingQueueFrameChannel;
import org.apache.druid.frame.channel.ReadableFileFrameChannel;
import org.apache.druid.frame.file.FrameFile;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexCursorFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableCauseMatcher;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.RoundingMode;
import java.util.List;

public class ChannelStageOutputReaderTest extends InitializedNullHandlingTest
{
  private static final int MAX_FRAMES = 10;
  private static final int EXPECTED_NUM_ROWS = 1209;

  private final BlockingQueueFrameChannel channel = new BlockingQueueFrameChannel(MAX_FRAMES);
  private final ChannelStageOutputReader reader = new ChannelStageOutputReader(channel.readable());

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private FrameReader frameReader;
  private List<Frame> frameList;

  @Before
  public void setUp()
  {
    final IncrementalIndex index = TestIndex.getIncrementalTestIndex();
    final IncrementalIndexCursorFactory cursorFactory = new IncrementalIndexCursorFactory(index);
    frameReader = FrameReader.create(cursorFactory.getRowSignature());
    frameList = FrameSequenceBuilder.fromCursorFactory(cursorFactory)
                                    .frameType(FrameType.ROW_BASED)
                                    .maxRowsPerFrame(IntMath.divide(index.size(), MAX_FRAMES, RoundingMode.CEILING))
                                    .frames()
                                    .toList();
  }

  @After
  public void tearDown()
  {
    reader.close();
  }

  @Test
  public void test_readLocally() throws IOException
  {
    writeAllFramesToChannel();

    Assert.assertSame(channel.readable(), reader.readLocally());
    reader.close(); // Won't close the channel, because it's already been returned by readLocally

    final int numRows = FrameTestUtil.readRowsFromFrameChannel(channel.readable(), frameReader).toList().size();
    Assert.assertEquals(EXPECTED_NUM_ROWS, numRows);
  }

  @Test
  public void test_readLocally_closePriorToRead() throws IOException
  {
    writeAllFramesToChannel();

    reader.close();

    // Can't read the channel after closing the reader
    Assert.assertThrows(
        IllegalStateException.class,
        reader::readLocally
    );
  }

  @Test
  public void test_readLocally_thenReadRemotely() throws IOException
  {
    writeAllFramesToChannel();

    Assert.assertSame(channel.readable(), reader.readLocally());

    // Can't read remotely after reading locally
    Assert.assertThrows(
        IllegalStateException.class,
        () -> reader.readRemotelyFrom(0)
    );

    // Can still read locally after this error
    final int numRows = FrameTestUtil.readRowsFromFrameChannel(channel.readable(), frameReader).toList().size();
    Assert.assertEquals(EXPECTED_NUM_ROWS, numRows);
  }

  @Test
  public void test_readRemotely_strideBasedOnReturnedChunk() throws IOException
  {
    // Test that reads entire chunks from readRemotelyFrom. This is a typical usage pattern.

    writeAllFramesToChannel();

    final File tmpFile = temporaryFolder.newFile();

    try (final FileOutputStream tmpOut = new FileOutputStream(tmpFile)) {
      int numReads = 0;
      long offset = 0;

      while (true) {
        try (final InputStream in = FutureUtils.getUnchecked(reader.readRemotelyFrom(offset), true)) {
          numReads++;
          final long bytesWritten = ByteStreams.copy(in, tmpOut);
          offset += bytesWritten;

          if (bytesWritten == 0) {
            break;
          }
        }
      }

      MatcherAssert.assertThat(numReads, Matchers.greaterThan(1));
    }

    final FrameFile frameFile = FrameFile.open(tmpFile, null);
    final int numRows =
        FrameTestUtil.readRowsFromFrameChannel(new ReadableFileFrameChannel(frameFile), frameReader).toList().size();

    Assert.assertEquals(EXPECTED_NUM_ROWS, numRows);
  }

  @Test
  public void test_readRemotely_strideOneByte() throws IOException
  {
    // Test that reads one byte at a time from readRemotelyFrom. This helps ensure that there are no edge cases
    // in the chunk-reading logic.

    writeAllFramesToChannel();

    final File tmpFile = temporaryFolder.newFile();

    try (final FileOutputStream tmpOut = new FileOutputStream(tmpFile)) {
      int numReads = 0;
      long offset = 0;

      while (true) {
        try (final InputStream in = FutureUtils.getUnchecked(reader.readRemotelyFrom(offset), true)) {
          numReads++;
          final int nextByte = in.read();

          if (nextByte < 0) {
            break;
          }

          tmpOut.write(nextByte);
          offset++;
        }
      }

      Assert.assertEquals(numReads, offset + 1);
    }

    final FrameFile frameFile = FrameFile.open(tmpFile, null);
    final int numRows =
        FrameTestUtil.readRowsFromFrameChannel(new ReadableFileFrameChannel(frameFile), frameReader).toList().size();

    Assert.assertEquals(EXPECTED_NUM_ROWS, numRows);
  }

  @Test
  public void test_readRemotely_thenLocally() throws IOException
  {
    writeAllFramesToChannel();

    // Read remotely
    FutureUtils.getUnchecked(reader.readRemotelyFrom(0), true);

    // Then read locally
    Assert.assertThrows(
        IllegalStateException.class,
        reader::readLocally
    );
  }

  @Test
  public void test_readRemotely_cannotReverse() throws IOException
  {
    writeAllFramesToChannel();

    // Read remotely from offset = 1.
    final InputStream in = FutureUtils.getUnchecked(reader.readRemotelyFrom(1), true);
    final int offset = ByteStreams.toByteArray(in).length;
    MatcherAssert.assertThat(offset, Matchers.greaterThan(0));

    // Then read again from offset = 0; should get an error.
    final RuntimeException e = Assert.assertThrows(
        RuntimeException.class,
        () -> FutureUtils.getUnchecked(reader.readRemotelyFrom(0), true)
    );

    MatcherAssert.assertThat(
        e,
        ThrowableCauseMatcher.hasCause(
            Matchers.allOf(
                CoreMatchers.instanceOf(IllegalStateException.class),
                ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith("Offset[0] no longer available"))
            )
        )
    );
  }

  private void writeAllFramesToChannel() throws IOException
  {
    for (Frame frame : frameList) {
      channel.writable().write(frame);
    }
    channel.writable().close();
  }
}
