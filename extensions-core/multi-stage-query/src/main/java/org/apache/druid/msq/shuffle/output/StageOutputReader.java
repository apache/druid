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

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.shuffle.input.WorkerOrLocalInputChannelFactory;

import java.io.Closeable;
import java.io.InputStream;

/**
 * Interface for remotely reading output channels for a particular stage. Each instance of this interface represents a
 * stream from a single {@link org.apache.druid.msq.kernel.StagePartition} in
 * {@link org.apache.druid.frame.file.FrameFile} format.
 */
public interface StageOutputReader extends Closeable
{
  /**
   * Returns an {@link InputStream} starting from a particular point in the
   * {@link org.apache.druid.frame.file.FrameFile}. Length of the stream is implementation-dependent; it may or may
   * not go all the way to the end of the file. Zero-length stream indicates EOF. Any nonzero length means you should
   * call this method again with a higher offset.
   *
   * @param offset offset into the frame file
   *
   * @see org.apache.druid.msq.exec.WorkerImpl#readChannel(StageId, int, long)
   */
  ListenableFuture<InputStream> readRemotelyFrom(long offset);

  /**
   * Returns a {@link ReadableFrameChannel} for local reading.
   *
   * @see WorkerOrLocalInputChannelFactory#openChannel(StageId, int, int)
   */
  ReadableFrameChannel readLocally();
}
