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

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.frame.processor.OutputChannel;
import org.apache.druid.frame.processor.PartitionedOutputChannel;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.ResourceLimitExceededException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * A composed writable channel to write frames. The channel can encapsulate multiple writable channels in it and
 * automatically switches to next channels once the current write channel cannot allow more writes.
 */
public class ComposingWritableFrameChannel implements WritableFrameChannel
{
  @Nullable
  private final List<Supplier<OutputChannel>> outputChannelSuppliers;

  @Nullable
  private final List<Supplier<PartitionedOutputChannel>> partitionedOutputChannelSuppliers;

  private final List<Supplier<WritableFrameChannel>> writableChannelSuppliers;
  private final Map<Integer, HashSet<Integer>> partitionToChannelMap;
  private int currentIndex;

  public ComposingWritableFrameChannel(
      @Nullable List<Supplier<OutputChannel>> outputChannelSuppliers,
      @Nullable List<Supplier<PartitionedOutputChannel>> partitionedOutputChannelSuppliers,
      List<Supplier<WritableFrameChannel>> writableChannelSuppliers,
      Map<Integer, HashSet<Integer>> partitionToChannelMap
  )
  {
    if (outputChannelSuppliers != null && partitionedOutputChannelSuppliers != null) {
      throw new IAE("Atmost one of outputChannelSuppliers and partitionedOutputChannelSuppliers can be provided");
    }
    this.outputChannelSuppliers = outputChannelSuppliers;
    this.partitionedOutputChannelSuppliers = partitionedOutputChannelSuppliers;
    this.writableChannelSuppliers = Preconditions.checkNotNull(writableChannelSuppliers, "writableChannelSuppliers is null");
    this.partitionToChannelMap =
        Preconditions.checkNotNull(partitionToChannelMap, "partitionToChannelMap is null");
    this.currentIndex = 0;
  }

  @Override
  public void write(FrameWithPartition frameWithPartition) throws IOException
  {
    if (currentIndex >= writableChannelSuppliers.size()) {
      throw new ISE("No more channels available to write. Total available channels : " + writableChannelSuppliers.size());
    }

    try {
      writableChannelSuppliers.get(currentIndex).get().write(frameWithPartition);
      partitionToChannelMap.computeIfAbsent(frameWithPartition.partition(), k -> Sets.newHashSetWithExpectedSize(1))
                           .add(currentIndex);
    }
    catch (ResourceLimitExceededException rlee) {
      // currently we're falling back to next available channel after we receive an RLEE. This is done so that the
      // exception is automatically passed up to the user incase all the channels are exhausted. If in future, more
      // cases come up to dictate control flow, then we can switch to returning a custom object from the channel's write
      // operation.
      writableChannelSuppliers.get(currentIndex).get().close();

      // We are converting the corresponding channel to read only after exhausting it because that channel won't be used
      // for writes anymore
      convertChannelSuppliersToReadOnly(currentIndex);

      currentIndex++;
      if (currentIndex >= writableChannelSuppliers.size()) {
        throw rlee;
      }
      write(frameWithPartition);
    }
  }

  private void convertChannelSuppliersToReadOnly(int index)
  {
    if (outputChannelSuppliers != null) {
      outputChannelSuppliers.get(index).get().convertToReadOnly();
    }
    if (partitionedOutputChannelSuppliers != null) {
      partitionedOutputChannelSuppliers.get(index).get().convertToReadOnly();
    }
  }

  @Override
  public void fail(@Nullable Throwable cause) throws IOException
  {
    for (Supplier<WritableFrameChannel> channel : writableChannelSuppliers) {
      channel.get().fail(cause);
    }
  }

  @Override
  public void close() throws IOException
  {
    if (currentIndex < writableChannelSuppliers.size()) {
      writableChannelSuppliers.get(currentIndex).get().close();
      convertChannelSuppliersToReadOnly(currentIndex);
      currentIndex = writableChannelSuppliers.size();
    }
  }

  @Override
  public boolean isClosed()
  {
    return currentIndex == writableChannelSuppliers.size();
  }

  @Override
  public ListenableFuture<?> writabilityFuture()
  {
    return writableChannelSuppliers.get(currentIndex).get().writabilityFuture();
  }
}
