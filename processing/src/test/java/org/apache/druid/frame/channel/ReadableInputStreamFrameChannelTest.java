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
import org.apache.druid.segment.incremental.IncrementalIndexCursorFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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

  @TempDir
  File temporaryFolder;

  final IncrementalIndexCursorFactory cursorFactory =
      new IncrementalIndexCursorFactory(TestIndex.getIncrementalTestIndex());

  ExecutorService executorService = Execs.singleThreaded("input-stream-fetcher-test");

  @Test
  public void testSimpleFrameFile()
  {
    InputStream inputStream = getInputStream();
    ReadableInputStreamFrameChannel readableInputStreamFrameChannel = ReadableInputStreamFrameChannel.open(
        inputStream,
        "readSimpleFrameFile",
        executorService,
        false,
        null
    );

    FrameTestUtil.assertRowsEqual(
        FrameTestUtil.readRowsFromCursorFactory(cursorFactory),
        FrameTestUtil.readRowsFromFrameChannel(
            readableInputStreamFrameChannel,
            FrameReader.create(cursorFactory.getRowSignature())
        )
    );
    Assertions.assertTrue(readableInputStreamFrameChannel.isFinished());
    readableInputStreamFrameChannel.close();

  }


  @Test
  public void testEmptyFrameFile() throws IOException
  {
    final File file = FrameTestUtil.writeFrameFile(
        Sequences.empty(),
        File.createTempFile("tmp", null, temporaryFolder)
    );
    ReadableInputStreamFrameChannel readableInputStreamFrameChannel = ReadableInputStreamFrameChannel.open(
        Files.newInputStream(file.toPath()),
        "readEmptyFrameFile",
        executorService,
        false,
        null
    );

    Assertions.assertEquals(
        0,
        FrameTestUtil.readRowsFromFrameChannel(
            readableInputStreamFrameChannel,
            FrameReader.create(cursorFactory.getRowSignature())
        ).toList().size()
    );
    Assertions.assertTrue(readableInputStreamFrameChannel.isFinished());
    readableInputStreamFrameChannel.close();
  }

  @Test
  public void testZeroBytesFrameFile() throws IOException
  {
    final File file = File.createTempFile("tmp", null, temporaryFolder);
    FileOutputStream outputStream = new FileOutputStream(file);
    outputStream.write(new byte[0]);
    outputStream.close();

    ReadableInputStreamFrameChannel readableInputStreamFrameChannel = ReadableInputStreamFrameChannel.open(
        Files.newInputStream(file.toPath()),
        "testZeroBytesFrameFile",
        executorService,
        false,
        null
    );

    final IllegalStateException e = Assertions.assertThrows(
        IllegalStateException.class,
        () ->
            FrameTestUtil.readRowsFromFrameChannel(
                readableInputStreamFrameChannel,
                FrameReader.create(cursorFactory.getRowSignature())
            ).toList()
    );

    MatcherAssert.assertThat(
        e.getMessage(),
        CoreMatchers.startsWith("Incomplete or missing frame at end of stream")
    );
  }

  @Test
  public void testTruncatedFrameFile() throws IOException
  {
    final int allocatorSize = 64000;
    final int truncatedSize = 30000; // Holds two full frames + one partial frame, after compression.

    final IncrementalIndexCursorFactory cursorFactory =
        new IncrementalIndexCursorFactory(TestIndex.getIncrementalTestIndex());

    final File file = FrameTestUtil.writeFrameFile(
        FrameSequenceBuilder.fromCursorFactory(cursorFactory)
                            .allocator(ArenaMemoryAllocator.create(ByteBuffer.allocate(allocatorSize)))
                            .frameType(FrameType.latestRowBased())
                            .frames(),
        File.createTempFile("tmp", null, temporaryFolder)
    );

    final byte[] truncatedFile = new byte[truncatedSize];

    try (final FileInputStream in = new FileInputStream(file)) {
      ByteStreams.readFully(in, truncatedFile);
    }


    ReadableInputStreamFrameChannel readableInputStreamFrameChannel = ReadableInputStreamFrameChannel.open(
        new ByteArrayInputStream(truncatedFile),
        "readTruncatedFrameFile",
        executorService,
        false,
        null
    );

    final ISE e = Assertions.assertThrows(ISE.class, () ->
        FrameTestUtil.readRowsFromFrameChannel(
            readableInputStreamFrameChannel,
            FrameReader.create(cursorFactory.getRowSignature())
        ).toList()
    );
    Assertions.assertTrue(e.getMessage().contains("Incomplete or missing frame at end of stream"));
  }

  @Test
  public void testIncorrectFrameFile() throws IOException
  {
    final File file = File.createTempFile("tmp", null, temporaryFolder);
    FileOutputStream outputStream = new FileOutputStream(file);
    outputStream.write(10);
    outputStream.close();

    ReadableInputStreamFrameChannel readableInputStreamFrameChannel = ReadableInputStreamFrameChannel.open(
        Files.newInputStream(file.toPath()),
        "readIncorrectFrameFile",
        executorService,
        false,
        null
    );

    final ISE e = Assertions.assertThrows(ISE.class, () ->
        FrameTestUtil.readRowsFromFrameChannel(
            readableInputStreamFrameChannel,
            FrameReader.create(cursorFactory.getRowSignature())
        ).toList()
    );
    Assertions.assertTrue(e.getMessage().contains("Incomplete or missing frame at end of stream"));

  }


  @Test
  public void closeInputStreamWhileReading() throws IOException
  {
    InputStream inputStream = getInputStream();
    ReadableInputStreamFrameChannel readableInputStreamFrameChannel = ReadableInputStreamFrameChannel.open(
        inputStream,
        "closeInputStreamWhileReading",
        executorService,
        false,
        null
    );
    inputStream.close();

    final ISE e = Assertions.assertThrows(ISE.class, () ->
        FrameTestUtil.assertRowsEqual(
            FrameTestUtil.readRowsFromCursorFactory(cursorFactory),
            FrameTestUtil.readRowsFromFrameChannel(
                readableInputStreamFrameChannel,
                FrameReader.create(cursorFactory.getRowSignature())
            )
        )
    );
    Assertions.assertTrue(e.getMessage().contains("Found error while reading input stream"));
  }

  @Test
  public void closeInputStreamWhileReadingCheckError() throws IOException, InterruptedException
  {
    InputStream inputStream = getInputStream();
    ReadableInputStreamFrameChannel readableInputStreamFrameChannel = ReadableInputStreamFrameChannel.open(
        inputStream,
        "closeInputStreamWhileReadingCheckError",
        executorService,
        false,
        null
    );

    inputStream.close();

    final ISE e = Assertions.assertThrows(ISE.class, () -> {
      while (!readableInputStreamFrameChannel.canRead()) {
        Thread.sleep(10);
      }
      readableInputStreamFrameChannel.readFrame();
    });
    Assertions.assertTrue(e.getMessage().contains("Found error while reading input stream"));
  }

  private InputStream getInputStream()
  {
    try {
      final File file = FrameTestUtil.writeFrameFile(
          FrameSequenceBuilder.fromCursorFactory(cursorFactory)
                              .maxRowsPerFrame(10)
                              .frameType(FrameType.latestRowBased())
                              .frames(),
          File.createTempFile("tmp", null, temporaryFolder)
      );
      return Files.newInputStream(file.toPath());
    }
    catch (IOException e) {
      throw new ISE(e, "Unable to create file input stream");
    }
  }
}
