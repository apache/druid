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
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import org.apache.druid.frame.channel.ReadableFileFrameChannel;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.file.FrameFile;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;

/**
 * Reader for {@link FrameFile} on disk.
 */
public class FileStageOutputReader implements StageOutputReader
{
  private final FrameFile frameFile;

  public FileStageOutputReader(FrameFile frameFile)
  {
    this.frameFile = frameFile;
  }

  @Override
  public ListenableFuture<InputStream> readRemotelyFrom(long offset)
  {
    try {
      final RandomAccessFile randomAccessFile = new RandomAccessFile(frameFile.file(), "r");

      if (offset >= randomAccessFile.length()) {
        randomAccessFile.close();
        return Futures.immediateFuture(new ByteArrayInputStream(ByteArrays.EMPTY_ARRAY));
      } else {
        randomAccessFile.seek(offset);
        return Futures.immediateFuture(Channels.newInputStream(randomAccessFile.getChannel()));
      }
    }
    catch (Exception e) {
      return Futures.immediateFailedFuture(e);
    }
  }

  @Override
  public ReadableFrameChannel readLocally()
  {
    return new ReadableFileFrameChannel(frameFile.newReference());
  }

  @Override
  public void close() throws IOException
  {
    frameFile.close();
  }
}
