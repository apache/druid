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

package org.apache.druid.msq.exec;

import org.apache.druid.frame.processor.OutputChannel;
import org.apache.druid.frame.processor.OutputChannelFactory;
import org.apache.druid.frame.processor.PartitionedOutputChannel;

import java.io.IOException;

/**
 * Decorator for {@link OutputChannelFactory} that notifies a {@link Listener} whenever a channel is opened.
 */
public class ListeningOutputChannelFactory implements OutputChannelFactory
{
  private final OutputChannelFactory delegate;
  private final Listener listener;

  public ListeningOutputChannelFactory(final OutputChannelFactory delegate, final Listener listener)
  {
    this.delegate = delegate;
    this.listener = listener;
  }

  @Override
  public OutputChannel openChannel(final int partitionNumber) throws IOException
  {
    return notifyListener(delegate.openChannel(partitionNumber));
  }


  @Override
  public OutputChannel openNilChannel(final int partitionNumber)
  {
    return notifyListener(delegate.openNilChannel(partitionNumber));
  }

  @Override
  public PartitionedOutputChannel openPartitionedChannel(
      final String name,
      final boolean deleteAfterRead
  )
  {
    throw new UnsupportedOperationException("Listening to partitioned channels is not supported");
  }

  @Override
  public boolean isBuffered()
  {
    return delegate.isBuffered();
  }

  private OutputChannel notifyListener(OutputChannel channel)
  {
    listener.channelOpened(channel);
    return channel;
  }

  public interface Listener
  {
    void channelOpened(OutputChannel channel);
  }
}
