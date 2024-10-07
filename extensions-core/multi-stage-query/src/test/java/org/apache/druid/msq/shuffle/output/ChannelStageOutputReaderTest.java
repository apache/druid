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

import com.google.common.collect.Iterables;
import com.google.common.io.ByteStreams;
import com.google.common.math.IntMath;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.channel.BlockingQueueFrameChannel;
import org.apache.druid.frame.channel.ReadableFileFrameChannel;
import org.apache.druid.frame.file.FrameFile;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.segment.QueryableIndexCursorFactory;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexCursorFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.utils.CloseableUtils;
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
import java.io.OutputStream;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.util.List;

public class ChannelStageOutputReaderTest extends InitializedNullHandlingTest
{
  /**
   * Tests that use {@link BlockingQueueFrameChannel#minimal()}.
   */
  public static class WithMinimalBuffering extends InitializedNullHandlingTest
  {
    private final Frame frame = Iterables.getOnlyElement(
        FrameSequenceBuilder
            .fromCursorFactory(new QueryableIndexCursorFactory(TestIndex.getNoRollupMMappedTestIndex()))
            .frameType(FrameType.ROW_BASED)
            .frames()
            .toList()
    );

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private BlockingQueueFrameChannel channel;
    private ChannelStageOutputReader channelReader;
    private File tmpFile;
    private OutputStream tmpOut;
    private FrameFile tmpFrameFile;

    // Variables used by doRead()
    private long offset;
    private ListenableFuture<InputStream> nextRead;

    @Before
    public void setUp() throws Exception
    {
      channel = BlockingQueueFrameChannel.minimal();
      channelReader = new ChannelStageOutputReader(channel.readable());
      tmpFile = temporaryFolder.newFile();
      tmpOut = Files.newOutputStream(tmpFile.toPath());
    }

    @After
    public void tearDown() throws Exception
    {
      CloseableUtils.closeAll(tmpOut, tmpFrameFile);
    }

    @Test
    public void test_remote_empty() throws Exception
    {
      // Close without writing anything.
      channel.writable().close();

      while (doRead(-1)) {
        // Do nothing, just keep reading.
      }

      Assert.assertEquals(0, tmpFrameFile.numFrames());
    }

    @Test
    public void test_remote_oneFrame() throws Exception
    {
      // Close after writing one frame.
      channel.writable().write(frame);
      channel.writable().close();

      while (doRead(-1)) {
        // Do nothing, just keep reading.
      }

      Assert.assertEquals(1, tmpFrameFile.numFrames());
      Assert.assertEquals(frame.numBytes(), tmpFrameFile.frame(0).numBytes());
    }

    @Test
    public void test_remote_oneFrame_writeAfterFirstRead() throws Exception
    {
      Assert.assertTrue(doRead(-1));

      // Close after writing one frame.
      channel.writable().write(frame);
      channel.writable().close();

      while (doRead(-1)) {
        // Do nothing, just keep reading.
      }

      Assert.assertEquals(1, tmpFrameFile.numFrames());
      Assert.assertEquals(frame.numBytes(), tmpFrameFile.frame(0).numBytes());
    }

    @Test
    public void test_remote_oneFrame_readOneByteAtATime() throws Exception
    {
      // Close after writing one frame.
      channel.writable().write(frame);
      channel.writable().close();

      while (doRead(1)) {
        // Do nothing, just keep reading.
      }

      Assert.assertEquals(1, tmpFrameFile.numFrames());
      Assert.assertEquals(frame.numBytes(), tmpFrameFile.frame(0).numBytes());
    }

    @Test
    public void test_remote_threeFrames_readOneByteAtATime() throws Exception
    {
      // Write one frame.
      channel.writable().write(frame);

      // See that we can't write another frame.
      final IllegalStateException e = Assert.assertThrows(
          IllegalStateException.class,
          () -> channel.writable().write(frame)
      );

      MatcherAssert.assertThat(
          e,
          ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith("Channel has no capacity"))
      );

      // Read the first frame until we start blocking.
      while (nextRead == null) {
        Assert.assertTrue(doRead(1));
      }

      // Write the next frame.
      Assert.assertFalse(nextRead.isDone());
      channel.writable().write(frame);

      // This write would have unblocked nextRead, which will now be done.
      Assert.assertTrue(nextRead.isDone());

      // Write a third frame.
      channel.writable().write(frame);

      // See that we can't write a fourth frame.
      final IllegalStateException e2 = Assert.assertThrows(
          IllegalStateException.class,
          () -> channel.writable().write(frame)
      );

      MatcherAssert.assertThat(
          e2,
          ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith("Channel has no capacity"))
      );

      // And read until we start blocking.
      while (nextRead == null) {
        Assert.assertTrue(doRead(1));
      }

      // Close.
      channel.writable().close();

      // Read until end of stream.
      while (doRead(1)) {
        // Just keep looping.
      }

      Assert.assertEquals(3, tmpFrameFile.numFrames());
      Assert.assertEquals(frame.numBytes(), tmpFrameFile.frame(0).numBytes());
      Assert.assertEquals(frame.numBytes(), tmpFrameFile.frame(1).numBytes());
      Assert.assertEquals(frame.numBytes(), tmpFrameFile.frame(2).numBytes());
    }

    /**
     * Do the next read operation.
     *
     * @return false if done reading, true if there's more to read
     */
    private boolean doRead(final long limit) throws IOException
    {
      if (nextRead == null) {
        nextRead = channelReader.readRemotelyFrom(offset);
      }

      if (nextRead.isDone()) {
        try (final InputStream in = FutureUtils.getUncheckedImmediately(nextRead)) {
          nextRead = null;
          long readSize = 0;

          if (limit == -1) {
            // Unlimited
            readSize = ByteStreams.copy(in, tmpOut);
          } else {
            // Limited
            while (readSize < limit) {
              final int r = in.read();
              if (r != -1) {
                readSize++;
                tmpOut.write(r);
              } else {
                break;
              }
            }
          }

          offset += readSize;

          if (readSize == 0) {
            channel.readable().close();
            tmpOut.close();
            tmpFrameFile = FrameFile.open(tmpFile, null);
            return false;
          }
        }
      }

      return true;
    }
  }

  /**
   * Tests that use {@link BlockingQueueFrameChannel} that is fully buffered.
   */
  public static class WithMaximalBuffering extends InitializedNullHandlingTest
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
      final IncrementalIndexCursorFactory adapter = new IncrementalIndexCursorFactory(index);
      frameReader = FrameReader.create(adapter.getRowSignature());
      frameList = FrameSequenceBuilder.fromCursorFactory(adapter)
                                      .frameType(FrameType.ROW_BASED)
                                      .maxRowsPerFrame(IntMath.divide(index.numRows(), MAX_FRAMES, RoundingMode.CEILING))
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

      try (final OutputStream tmpOut = Files.newOutputStream(tmpFile.toPath())) {
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
}
