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

package org.apache.druid.frame.channel;

import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@RunWith(Enclosed.class)
public class ReadableByteChunksFrameChannelTest
{
  /**
   * Non-parameterized test cases. Each one is special.
   */
  public static class NonParameterizedTests extends InitializedNullHandlingTest
  {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testZeroBytes()
    {
      final ReadableByteChunksFrameChannel channel = ReadableByteChunksFrameChannel.create("test");
      channel.doneWriting();

      Assert.assertTrue(channel.canRead());
      Assert.assertFalse(channel.isFinished());
      Assert.assertTrue(channel.isErrorOrFinished());

      expectedException.expect(IllegalStateException.class);
      expectedException.expectMessage("Incomplete or missing frame at end of stream (id = test, position = 0)");

      channel.read();
    }

    @Test
    public void testZeroBytesWithSpecialError()
    {
      final ReadableByteChunksFrameChannel channel = ReadableByteChunksFrameChannel.create("test");
      channel.setError(new IllegalArgumentException("test error"));
      channel.doneWriting();

      Assert.assertTrue(channel.canRead());
      Assert.assertFalse(channel.isFinished());
      Assert.assertTrue(channel.isErrorOrFinished());

      expectedException.expect(IllegalArgumentException.class);
      expectedException.expectMessage("test error");

      channel.read();
    }

    @Test
    public void testEmptyFrameFile() throws IOException
    {
      // File with no frames (but still well-formed).
      final File file = FrameTestUtil.writeFrameFile(Sequences.empty(), temporaryFolder.newFile());

      final ReadableByteChunksFrameChannel channel = ReadableByteChunksFrameChannel.create("test");
      channel.addChunk(Files.toByteArray(file));
      channel.doneWriting();
      Assert.assertEquals(file.length(), channel.getBytesAdded());

      while (channel.canRead()) {
        Assert.assertFalse(channel.isFinished());
        Assert.assertFalse(channel.isErrorOrFinished());
        channel.read();
      }

      Assert.assertTrue(channel.isFinished());
      channel.close();
    }

    @Test
    public void testTruncatedFrameFile() throws IOException
    {
      final int allocatorSize = 64000;
      final int truncatedSize = 30000; // Holds two full columnar frames + one partial frame, after compression.

      final IncrementalIndexStorageAdapter adapter =
          new IncrementalIndexStorageAdapter(TestIndex.getIncrementalTestIndex());

      final File file = FrameTestUtil.writeFrameFile(
          FrameSequenceBuilder.fromAdapter(adapter)
                              .allocator(ArenaMemoryAllocator.create(ByteBuffer.allocate(allocatorSize)))
                              .frameType(FrameType.COLUMNAR) // No particular reason to test with both frame types
                              .frames(),
          temporaryFolder.newFile()
      );

      final byte[] truncatedFile = new byte[truncatedSize];

      try (final FileInputStream in = new FileInputStream(file)) {
        ByteStreams.readFully(in, truncatedFile);
      }

      final ReadableByteChunksFrameChannel channel = ReadableByteChunksFrameChannel.create("test");
      channel.addChunk(truncatedFile);
      channel.doneWriting();
      Assert.assertEquals(truncatedFile.length, channel.getBytesAdded());

      Assert.assertTrue(channel.canRead());
      Assert.assertFalse(channel.isFinished());
      Assert.assertFalse(channel.isErrorOrFinished());
      channel.read(); // Throw away value.

      Assert.assertTrue(channel.canRead());
      Assert.assertFalse(channel.isFinished());
      Assert.assertFalse(channel.isErrorOrFinished());
      channel.read(); // Throw away value.

      Assert.assertTrue(channel.canRead());
      Assert.assertFalse(channel.isFinished());
      Assert.assertTrue(channel.isErrorOrFinished());

      expectedException.expect(IllegalStateException.class);
      expectedException.expectMessage(CoreMatchers.startsWith("Incomplete or missing frame at end of stream"));
      channel.read();
    }

    @Test
    public void testSetError() throws IOException
    {
      final int allocatorSize = 64000;
      final int errorAtBytePosition = 30000; // Holds two full frames + one partial frame, after compression.

      final IncrementalIndexStorageAdapter adapter =
          new IncrementalIndexStorageAdapter(TestIndex.getIncrementalTestIndex());

      final File file = FrameTestUtil.writeFrameFile(
          FrameSequenceBuilder.fromAdapter(adapter)
                              .allocator(ArenaMemoryAllocator.create(ByteBuffer.allocate(allocatorSize)))
                              .frameType(FrameType.COLUMNAR) // No particular reason to test with both frame types
                              .frames(),
          temporaryFolder.newFile()
      );

      final ReadableByteChunksFrameChannel channel = ReadableByteChunksFrameChannel.create("test");
      final byte[] fileBytes = Files.toByteArray(file);
      final byte[] chunk1 = new byte[errorAtBytePosition];
      System.arraycopy(fileBytes, 0, chunk1, 0, chunk1.length);
      channel.addChunk(chunk1);
      Assert.assertEquals(chunk1.length, channel.getBytesAdded());

      channel.setError(new ISE("Test error!"));
      channel.doneWriting();
      Assert.assertEquals(chunk1.length, channel.getBytesAdded());

      expectedException.expect(IllegalStateException.class);
      expectedException.expectMessage("Test error!");
      channel.read();
    }
  }

  /**
   * Parameterized test cases that use various FrameFiles built from {@link TestIndex#getIncrementalTestIndex()}.
   */
  @RunWith(Parameterized.class)
  public static class ParameterizedWithTestIndexTests extends InitializedNullHandlingTest
  {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final FrameType frameType;
    private final int maxRowsPerFrame;
    private final int chunkSize;

    public ParameterizedWithTestIndexTests(final FrameType frameType, final int maxRowsPerFrame, final int chunkSize)
    {
      this.frameType = frameType;
      this.maxRowsPerFrame = maxRowsPerFrame;
      this.chunkSize = chunkSize;
    }

    @Parameterized.Parameters(name = "frameType = {0}, maxRowsPerFrame = {1}, chunkSize = {2}")
    public static Iterable<Object[]> constructorFeeder()
    {
      final List<Object[]> constructors = new ArrayList<>();

      for (FrameType frameType : FrameType.values()) {
        for (int maxRowsPerFrame : new int[]{1, 50, Integer.MAX_VALUE}) {
          for (int chunkSize : new int[]{1, 10, 1_000, 5_000, 50_000, 1_000_000}) {
            constructors.add(new Object[]{frameType, maxRowsPerFrame, chunkSize});
          }
        }
      }

      return constructors;
    }

    @Test
    public void testWriteFullyThenRead() throws IOException
    {
      // Create a frame file.
      final IncrementalIndexStorageAdapter adapter =
          new IncrementalIndexStorageAdapter(TestIndex.getIncrementalTestIndex());

      final File file = FrameTestUtil.writeFrameFile(
          FrameSequenceBuilder.fromAdapter(adapter)
                              .maxRowsPerFrame(maxRowsPerFrame)
                              .frameType(frameType)
                              .frames(),
          temporaryFolder.newFile()
      );

      final ReadableByteChunksFrameChannel channel = ReadableByteChunksFrameChannel.create("test");
      ListenableFuture<?> firstBackpressureFuture = null;

      long totalSize = 0;
      Assert.assertEquals(0, channel.getBytesBuffered());

      try (final Chunker chunker = new Chunker(new FileInputStream(file), chunkSize)) {
        byte[] chunk;

        while ((chunk = chunker.nextChunk()) != null) {
          totalSize += chunk.length;

          final ListenableFuture<?> backpressureFuture = channel.addChunk(chunk);
          Assert.assertEquals(channel.getBytesAdded(), totalSize);

          // Minimally-sized channel means backpressure is exerted as soon as a single frame is available.
          Assert.assertEquals(channel.canRead(), backpressureFuture != null);

          if (backpressureFuture != null) {
            if (firstBackpressureFuture == null) {
              firstBackpressureFuture = backpressureFuture;
            } else {
              Assert.assertSame(firstBackpressureFuture, backpressureFuture);
            }
          }
        }

        // Backpressure should be exerted right now, since this is a minimal channel with at least one full frame in it.
        Assert.assertNotNull(firstBackpressureFuture);
        Assert.assertFalse(firstBackpressureFuture.isDone());

        channel.doneWriting();
      }

      FrameTestUtil.assertRowsEqual(
          FrameTestUtil.readRowsFromAdapter(adapter, null, false),
          FrameTestUtil.readRowsFromFrameChannel(channel, FrameReader.create(adapter.getRowSignature()))
      );
    }

    @Test
    public void testWriteReadInterleaved() throws IOException
    {
      // Create a frame file.
      final IncrementalIndexStorageAdapter adapter =
          new IncrementalIndexStorageAdapter(TestIndex.getIncrementalTestIndex());

      final File file = FrameTestUtil.writeFrameFile(
          FrameSequenceBuilder.fromAdapter(adapter)
                              .maxRowsPerFrame(maxRowsPerFrame)
                              .frameType(frameType)
                              .frames(),
          temporaryFolder.newFile()
      );

      final ReadableByteChunksFrameChannel channel = ReadableByteChunksFrameChannel.create("test");
      final BlockingQueueFrameChannel outChannel = new BlockingQueueFrameChannel(10_000); // Enough to hold all frames
      ListenableFuture<?> backpressureFuture = null;

      int iteration = 0;
      long totalSize = 0;

      try (final Chunker chunker = new Chunker(new FileInputStream(file), chunkSize)) {
        byte[] chunk;

        while ((chunk = chunker.nextChunk()) != null) {
          // Read one frame every 3 iterations. Read everything every 11 iterations. Otherwise, read nothing.
          if (iteration % 3 == 0) {
            while (channel.canRead()) {
              outChannel.writable().write(channel.read());
            }

            // After reading everything, backpressure should be off.
            Assert.assertTrue(backpressureFuture == null || backpressureFuture.isDone());
          } else if (iteration % 11 == 0) {
            if (channel.canRead()) {
              outChannel.writable().write(channel.read());
            }
          }

          if (backpressureFuture != null && backpressureFuture.isDone()) {
            backpressureFuture = null;
          }

          iteration++;
          totalSize += chunk.length;

          // Write next chunk.
          final ListenableFuture<?> addVal = channel.addChunk(chunk);
          Assert.assertEquals(totalSize, channel.getBytesAdded());

          // Minimally-sized channel means backpressure is exerted as soon as a single frame is available.
          Assert.assertEquals(channel.canRead(), addVal != null);

          if (addVal != null) {
            if (backpressureFuture == null) {
              backpressureFuture = addVal;
            } else {
              Assert.assertSame(backpressureFuture, addVal);
            }
          }
        }

        channel.doneWriting();

        // Get all the remaining frames.
        while (channel.canRead()) {
          outChannel.writable().write(channel.read());
        }

        outChannel.writable().close();
      }

      FrameTestUtil.assertRowsEqual(
          FrameTestUtil.readRowsFromAdapter(adapter, null, false),
          FrameTestUtil.readRowsFromFrameChannel(outChannel.readable(), FrameReader.create(adapter.getRowSignature()))
      );
    }

    private static class Chunker implements Closeable
    {
      private final FileInputStream in;
      private final int chunkSize;
      private final byte[] buf;
      private boolean eof = false;

      public Chunker(final FileInputStream in, final int chunkSize)
      {
        this.in = in;
        this.chunkSize = chunkSize;
        this.buf = new byte[chunkSize];
      }

      @Nullable
      public byte[] nextChunk() throws IOException
      {
        if (eof) {
          return null;
        }

        int p = 0;
        while (p < chunkSize) {
          final int r = in.read(buf, p, chunkSize - p);

          if (r < 0) {
            eof = true;
            break;
          } else {
            p += r;
          }
        }

        if (p > 0) {
          return Arrays.copyOf(buf, p);
        } else {
          return null;
        }
      }

      @Override
      public void close() throws IOException
      {
        in.close();
      }
    }
  }
}
