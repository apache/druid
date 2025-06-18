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

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.Int2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectSortedMap;
import it.unimi.dsi.fastutil.ints.IntSortedSet;
import org.apache.druid.frame.channel.ReadableFrameChannel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A list of {@link OutputChannel}.
 *
 * The advantage of this class over {@code List<OutputChannel>} is that it has extra convenience methods
 * like {@link #getChannelsForPartition} and {@link #readOnly()}.
 */
public class OutputChannels
{
  private final List<OutputChannel> outputChannels;
  private final Int2ObjectSortedMap<List<OutputChannel>> partitionToChannelMap;

  private OutputChannels(final List<OutputChannel> outputChannels)
  {
    this.outputChannels = outputChannels;

    this.partitionToChannelMap = new Int2ObjectRBTreeMap<>();

    for (final OutputChannel outputChannel : outputChannels) {
      partitionToChannelMap.computeIfAbsent(outputChannel.getPartitionNumber(), ignored -> new ArrayList<>())
                           .add(outputChannel);
    }
  }

  public static OutputChannels none()
  {
    return wrap(Collections.emptyList());
  }

  /**
   * Creates an instance wrapping all the provided channels.
   */
  public static OutputChannels wrap(final List<OutputChannel> outputChannels)
  {
    return new OutputChannels(outputChannels);
  }

  /**
   * Creates an instance wrapping read-only versions (see {@link OutputChannel#readOnly()}) of all the
   * provided channels.
   */
  public static OutputChannels wrapReadOnly(final List<OutputChannel> outputChannels)
  {
    return new OutputChannels(outputChannels.stream().map(OutputChannel::readOnly).collect(Collectors.toList()));
  }

  /**
   * Verifies there is exactly one channel per partition.
   */
  public OutputChannels verifySingleChannel()
  {
    for (int partitionNumber : getPartitionNumbers()) {
      final List<OutputChannel> outputChannelsForPartition =
          getChannelsForPartition(partitionNumber);

      Preconditions.checkState(partitionNumber >= 0, "Expected partitionNumber >= 0, but got [%s]", partitionNumber);
      Preconditions.checkState(
          outputChannelsForPartition.size() == 1,
          "Expected one channel for partition [%s], but got [%s]",
          partitionNumber,
          outputChannelsForPartition.size()
      );
    }
    return this;
  }

  /**
   * Returns the set of partition numbers that appear across all channels.
   */
  public IntSortedSet getPartitionNumbers()
  {
    return partitionToChannelMap.keySet();
  }

  /**
   * Returns all channels.
   */
  public List<OutputChannel> getAllChannels()
  {
    return outputChannels;
  }

  /**
   * Returns all channels, as readable channels.
   */
  public List<ReadableFrameChannel> getAllReadableChannels()
  {
    return outputChannels.stream().map(OutputChannel::getReadableChannel).collect(Collectors.toList());
  }

  /**
   * Returns channels for which {@link OutputChannel#getPartitionNumber()} returns {@code partitionNumber}.
   */
  public List<OutputChannel> getChannelsForPartition(final int partitionNumber)
  {
    final List<OutputChannel> retVal = partitionToChannelMap.get(partitionNumber);

    if (retVal != null) {
      return retVal;
    } else {
      return Collections.emptyList();
    }
  }

  /**
   * Returns a read-only version of this instance. Each individual output channel is replaced with its
   * read-only version ({@link OutputChannel#readOnly()}, which reduces memory usage.
   */
  public OutputChannels readOnly()
  {
    return wrapReadOnly(outputChannels);
  }
}
