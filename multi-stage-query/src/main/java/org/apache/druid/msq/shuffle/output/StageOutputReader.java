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
import org.apache.druid.frame.file.FrameFile;
import org.apache.druid.msq.exec.Worker;
import org.apache.druid.msq.kernel.StageId;

import java.io.Closeable;
import java.io.InputStream;

/**
 * Interface for reading output channels for a particular stage. Each instance of this interface represents a
 * stream from a single {@link org.apache.druid.msq.kernel.StagePartition} in {@link FrameFile} format.
 *
 * @see FileStageOutputReader implementation backed by {@link FrameFile}
 * @see ChannelStageOutputReader implementation backed by {@link ReadableFrameChannel}
 * @see NilStageOutputReader implementation for an empty channel
 */
public interface StageOutputReader extends Closeable
{
  /**
   * Method for remote reads.
   *
   * This method ultimately backs {@link Worker#readStageOutput(StageId, int, long)}. Refer to that method's
   * documentation for details about behavior of the returned future.
   *
   * Callers are responsible for closing the returned {@link InputStream}. This input stream may encapsulate
   * resources that are not closed by this class's {@link #close()} method.
   *
   * It is implementation-dependent whether calls to this method must have monotonically increasing offsets.
   * In particular, {@link ChannelStageOutputReader} requires monotonically increasing offsets, but
   * {@link FileStageOutputReader} and {@link NilStageOutputReader} do not.
   *
   * @param offset offset into the stage output file
   *
   * @see StageOutputHolder#readRemotelyFrom(long) which uses this method
   * @see Worker#readStageOutput(StageId, int, long) for documentation on behavior of the returned future
   */
  ListenableFuture<InputStream> readRemotelyFrom(long offset);

  /**
   * Method for local reads.
   *
   * Depending on implementation, this method may or may not be able to be called multiple times, and may or may not
   * be able to be mixed with {@link #readRemotelyFrom(long)}. Refer to the specific implementation for more details.
   *
   * Callers are responsible for closing the returned channel. The returned channel may encapsulate resources that
   * are not closed by this class's {@link #close()} method.
   *
   * It is implementation-dependent whether this method can be called multiple times. In particular,
   * {@link ChannelStageOutputReader#readLocally()} can only be called one time, but the implementations in
   * {@link FileStageOutputReader} and {@link NilStageOutputReader} can be called multiple times.
   *
   * @see StageOutputHolder#readLocally() which uses this method
   */
  ReadableFrameChannel readLocally();
}
