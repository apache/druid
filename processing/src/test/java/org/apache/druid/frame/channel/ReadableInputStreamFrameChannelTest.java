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
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.concurrent.ExecutorService;

public class ReadableInputStreamFrameChannelTest extends InitializedNullHandlingTest
{

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  final IncrementalIndexStorageAdapter adapter =
      new IncrementalIndexStorageAdapter(TestIndex.getIncrementalTestIndex());

  ExecutorService executorService = Execs.singleThreaded("input-stream-fetcher-test");

  @Test
  public void testSimpleFrameFile()
  {
    InputStream inputStream = getInputStream();
    ReadableInputStreamFrameChannel readableInputStreamFrameChannel = ReadableInputStreamFrameChannel.open(
        inputStream,
        "readSimpleFrameFile",
        executorService
    );

    FrameTestUtil.assertRowsEqual(
        FrameTestUtil.readRowsFromAdapter(adapter, null, false),
        FrameTestUtil.readRowsFromFrameChannel(
            readableInputStreamFrameChannel,
            FrameReader.create(adapter.getRowSignature())
        )
    );
    Assert.assertTrue(readableInputStreamFrameChannel.isFinished());
    readableInputStreamFrameChannel.close();

  }


  @Test
  public void testEmptyFrameFile() throws IOException
  {
    final File file = FrameTestUtil.writeFrameFile(Sequences.empty(), temporaryFolder.newFile());
    ReadableInputStreamFrameChannel readableInputStreamFrameChannel = ReadableInputStreamFrameChannel.open(
        Files.newInputStream(file.toPath()),
        "readEmptyFrameFile",
        executorService
    );

    Assert.assertEquals(FrameTestUtil.readRowsFromFrameChannel(
        readableInputStreamFrameChannel,
        FrameReader.create(adapter.getRowSignature())
    ).toList().size(), 0);
    Assert.assertTrue(readableInputStreamFrameChannel.isFinished());
    readableInputStreamFrameChannel.close();
  }

  @Test
  public void testZeroBytesFrameFile() throws IOException
  {
    final File file = temporaryFolder.newFile();
    FileOutputStream outputStream = new FileOutputStream(file);
    outputStream.write(new byte[0]);
    outputStream.close();

    ReadableInputStreamFrameChannel readableInputStreamFrameChannel = ReadableInputStreamFrameChannel.open(
        Files.newInputStream(file.toPath()),
        "testZeroBytesFrameFile",
        executorService
    );

    final IllegalStateException e = Assert.assertThrows(
        IllegalStateException.class,
        () ->
            FrameTestUtil.readRowsFromFrameChannel(
                readableInputStreamFrameChannel,
                FrameReader.create(adapter.getRowSignature())
            ).toList()
    );

    MatcherAssert.assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith("Incomplete or missing frame at end of stream"))
    );
  }

  @Test
  public void testTruncatedFrameFile() throws IOException
  {
    final int allocatorSize = 64000;
    final int truncatedSize = 30000; // Holds two full frames + one partial frame, after compression.

    final IncrementalIndexStorageAdapter adapter =
        new IncrementalIndexStorageAdapter(TestIndex.getIncrementalTestIndex());

    final File file = FrameTestUtil.writeFrameFile(
        FrameSequenceBuilder.fromAdapter(adapter)
                            .allocator(ArenaMemoryAllocator.create(ByteBuffer.allocate(allocatorSize)))
                            .frameType(FrameType.ROW_BASED)
                            .frames(),
        temporaryFolder.newFile()
    );

    final byte[] truncatedFile = new byte[truncatedSize];

    try (final FileInputStream in = new FileInputStream(file)) {
      ByteStreams.readFully(in, truncatedFile);
    }


    ReadableInputStreamFrameChannel readableInputStreamFrameChannel = ReadableInputStreamFrameChannel.open(
        new ByteArrayInputStream(truncatedFile),
        "readTruncatedFrameFile",
        executorService
    );

    expectedException.expect(ISE.class);
    expectedException.expectMessage("Incomplete or missing frame at end of stream");

    Assert.assertEquals(FrameTestUtil.readRowsFromFrameChannel(
        readableInputStreamFrameChannel,
        FrameReader.create(adapter.getRowSignature())
    ).toList().size(), 0);
    Assert.assertTrue(readableInputStreamFrameChannel.isFinished());
    readableInputStreamFrameChannel.close();
  }

  @Test
  public void testIncorrectFrameFile() throws IOException
  {
    final File file = temporaryFolder.newFile();
    FileOutputStream outputStream = new FileOutputStream(file);
    outputStream.write(10);
    outputStream.close();

    ReadableInputStreamFrameChannel readableInputStreamFrameChannel = ReadableInputStreamFrameChannel.open(
        Files.newInputStream(file.toPath()),
        "readIncorrectFrameFile",
        executorService
    );

    expectedException.expect(ISE.class);
    expectedException.expectMessage("Incomplete or missing frame at end of stream");

    Assert.assertEquals(FrameTestUtil.readRowsFromFrameChannel(
        readableInputStreamFrameChannel,
        FrameReader.create(adapter.getRowSignature())
    ).toList().size(), 0);
    Assert.assertTrue(readableInputStreamFrameChannel.isFinished());
    readableInputStreamFrameChannel.close();

  }


  @Test
  public void closeInputStreamWhileReading() throws IOException
  {
    InputStream inputStream = getInputStream();
    ReadableInputStreamFrameChannel readableInputStreamFrameChannel = ReadableInputStreamFrameChannel.open(
        inputStream,
        "closeInputStreamWhileReading",
        executorService
    );
    inputStream.close();

    expectedException.expect(ISE.class);
    expectedException.expectMessage("Found error while reading input stream");
    FrameTestUtil.assertRowsEqual(
        FrameTestUtil.readRowsFromAdapter(adapter, null, false),
        FrameTestUtil.readRowsFromFrameChannel(
            readableInputStreamFrameChannel,
            FrameReader.create(adapter.getRowSignature())
        )
    );
    Assert.assertTrue(readableInputStreamFrameChannel.isFinished());
    readableInputStreamFrameChannel.close();
  }

  @Test
  public void closeInputStreamWhileReadingCheckError() throws IOException, InterruptedException
  {
    InputStream inputStream = getInputStream();
    ReadableInputStreamFrameChannel readableInputStreamFrameChannel = ReadableInputStreamFrameChannel.open(
        inputStream,
        "closeInputStreamWhileReadingCheckError",
        executorService
    );

    inputStream.close();

    expectedException.expect(ISE.class);
    expectedException.expectMessage("Found error while reading input stream");

    while (!readableInputStreamFrameChannel.canRead()) {
      Thread.sleep(10);
    }
    readableInputStreamFrameChannel.read();
    Assert.assertTrue(readableInputStreamFrameChannel.isFinished());
    readableInputStreamFrameChannel.close();
  }

  private InputStream getInputStream()
  {
    try {
      final File file = FrameTestUtil.writeFrameFile(
          FrameSequenceBuilder.fromAdapter(adapter).maxRowsPerFrame(10).frameType(FrameType.ROW_BASED).frames(),
          temporaryFolder.newFile()
      );
      return Files.newInputStream(file.toPath());
    }
    catch (IOException e) {
      throw new ISE(e, "Unable to create file input stream");
    }
  }
}
