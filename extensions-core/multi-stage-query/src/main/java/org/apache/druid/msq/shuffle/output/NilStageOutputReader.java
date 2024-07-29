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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.frame.channel.ByteTracker;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.ReadableNilFrameChannel;
import org.apache.druid.frame.file.FrameFileWriter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;

/**
 * Reader for the case where stage output is known to be empty.
 */
public class NilStageOutputReader implements StageOutputReader
{
  public static final NilStageOutputReader INSTANCE = new NilStageOutputReader();

  private static final byte[] EMPTY_FRAME_FILE;

  static {
    try {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      FrameFileWriter.open(Channels.newChannel(baos), null, ByteTracker.unboundedTracker()).close();
      EMPTY_FRAME_FILE = baos.toByteArray();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ListenableFuture<InputStream> readRemotelyFrom(final long offset)
  {
    final ByteArrayInputStream in = new ByteArrayInputStream(EMPTY_FRAME_FILE);

    //noinspection ResultOfMethodCallIgnored: OK to ignore since "skip" always works for ByteArrayInputStream.
    in.skip(offset);

    return Futures.immediateFuture(in);
  }

  @Override
  public ReadableFrameChannel readLocally()
  {
    return ReadableNilFrameChannel.INSTANCE;
  }

  @Override
  public void close()
  {
    // Nothing to do.
  }
}
