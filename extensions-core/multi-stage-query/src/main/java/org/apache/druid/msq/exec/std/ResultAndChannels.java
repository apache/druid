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

package org.apache.druid.msq.exec.std;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.frame.channel.BlockingQueueFrameChannel;
import org.apache.druid.frame.processor.OutputChannelFactory;
import org.apache.druid.frame.processor.OutputChannels;
import org.apache.druid.msq.exec.StageProcessor;

/**
 * Represents the completion of some processing ({@link #resultFuture()}) and the output channels associated
 * with that processing ({@link #outputChannels()}).
 *
 * @see StageProcessor class-level javadoc for future discussion of the meaning of "result" and "output channels"
 */
public class ResultAndChannels<T>
{
  private final ListenableFuture<T> result;
  private final OutputChannels outputChannels;

  public ResultAndChannels(
      final ListenableFuture<T> result,
      final OutputChannels outputChannels
  )
  {
    this.result = result;
    this.outputChannels = outputChannels;
  }

  /**
   * Future that represents the completion of processing. When this future resolves, the output has been fully
   * generated and all processing has stopped.
   */
  public ListenableFuture<T> resultFuture()
  {
    return result;
  }

  /**
   * Processed outputs. If unbuffered (e.g. {@link BlockingQueueFrameChannel} these are readable prior to
   * {@link #resultFuture()} resolving. If buffered, these are readable after {@link #resultFuture()} resolves.
   *
   * This class does not itself tell you if the output channels are buffered. This can be known by checking
   * {@link OutputChannelFactory#isBuffered()} on the factory that created them.
   */
  public OutputChannels outputChannels()
  {
    return outputChannels;
  }
}
