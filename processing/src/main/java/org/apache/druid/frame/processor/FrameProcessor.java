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

package org.apache.druid.frame.processor;

import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;

import java.io.IOException;
import java.util.List;

/**
 * A FrameProcessor is like an incremental version of Runnable that operates on {@link ReadableFrameChannel} and
 * {@link WritableFrameChannel}.
 *
 * It is designed to enable interleaved non-blocking work on a fixed-size thread pool. Typically, this is done using
 * an instance of {@link FrameProcessorExecutor}.
 */
public interface FrameProcessor<T>
{
  /**
   * List of input channels. The positions of channels in this list are used to build the {@code readableInputs} set
   * provided to {@link #runIncrementally}.
   */
  List<ReadableFrameChannel> inputChannels();

  /**
   * List of output channels.
   */
  List<WritableFrameChannel> outputChannels();

  /**
   * Runs some of the algorithm, without blocking, and either returns a value or a set of input channels
   * to wait for. This method is called by {@link FrameProcessorExecutor#runFully} when all output channels are
   * writable. Therefore, it is guaranteed that each output channel can accept at least one frame.
   *
   * This method must not read more than one frame from each readable input channel, and must not write more than one
   * frame to each output channel.
   *
   * @param readableInputs channels from {@link #inputChannels()} that are either finished or ready to read.
   *                       That is: either {@link ReadableFrameChannel#isFinished()} or
   *                       {@link ReadableFrameChannel#canRead()} are true.
   *
   * @return either a final return value or a set of input channels to wait for. Must be nonnull.
   */
  ReturnOrAwait<T> runIncrementally(IntSet readableInputs) throws InterruptedException, IOException;

  /**
   * Closes resources used by this worker.
   *
   * Exact same concept as {@link java.io.Closeable#close()}. This interface does not extend Closeable, in order to
   * make it easier to find all places where cleanup happens. (Static analysis tools can lose the thread when Closeables
   * are closed in generic ways.)
   *
   * Implementations typically call {@link ReadableFrameChannel#close()} and
   * {@link WritableFrameChannel#close()} on all input and output channels, as well as releasing any additional
   * resources that may be held.
   *
   * In cases of cancellation, this method may be called even if {@link #runIncrementally} has not yet returned a
   * result via {@link ReturnOrAwait#returnObject}.
   */
  void cleanup() throws IOException;
}
