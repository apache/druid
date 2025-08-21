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
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.ReadableNilFrameChannel;
import org.apache.druid.frame.file.FrameFile;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.ExecutionException;

public class NilStageOutputReaderTest extends InitializedNullHandlingTest
{
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final NilStageOutputReader reader = NilStageOutputReader.INSTANCE;

  @Test
  public void test_readRemotelyFrom_zeroOffset() throws IOException, ExecutionException, InterruptedException
  {
    final ListenableFuture<InputStream> future = reader.readRemotelyFrom(0L);
    final File tmpFile = temporaryFolder.newFile();

    try (final InputStream in = future.get()) {
      Files.copy(in, tmpFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
    }

    try (final FrameFile frameFile = FrameFile.open(tmpFile, null)) {
      Assert.assertEquals(0, frameFile.numFrames());
    }
  }

  @Test
  public void test_readRemotelyFrom_nonZeroOffset() throws IOException, ExecutionException, InterruptedException
  {
    final long offset = 5L;

    final File tmpFile = temporaryFolder.newFile();
    try (final OutputStream out = new FileOutputStream(tmpFile)) {
      // write from beginning to offset 5
      try (final InputStream in = reader.readRemotelyFrom(0).get()) {
        for (int i = 0; i < offset; i++) {
          final int r = in.read();
          MatcherAssert.assertThat(r, Matchers.greaterThanOrEqualTo(0));
          out.write(r);
        }
      }

      // write from offset 5 to the end
      try (final InputStream in = reader.readRemotelyFrom(offset).get()) {
        ByteStreams.copy(in, out);
      }
    }

    // Verify written file
    try (final FrameFile frameFile = FrameFile.open(tmpFile, null)) {
      Assert.assertEquals(0, frameFile.numFrames());
    }
  }

  @Test
  public void test_readRemotelyFrom_offsetBeyondLength() throws IOException, ExecutionException, InterruptedException
  {
    final ListenableFuture<InputStream> future = reader.readRemotelyFrom(1000L);

    try (final InputStream inputStream = future.get()) {
      Assert.assertEquals(-1, inputStream.read()); // expect EOF
    }
  }

  @Test
  public void test_readLocally()
  {
    final ReadableFrameChannel channel = reader.readLocally();
    Assert.assertSame(ReadableNilFrameChannel.INSTANCE, channel);
  }
}
