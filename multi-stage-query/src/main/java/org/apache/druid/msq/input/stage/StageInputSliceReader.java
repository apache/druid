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

package org.apache.druid.msq.input.stage;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.msq.counters.CounterNames;
import org.apache.druid.msq.counters.CounterTracker;
import org.apache.druid.msq.indexing.CountingReadableFrameChannel;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSliceReader;
import org.apache.druid.msq.input.ReadableInput;
import org.apache.druid.msq.input.ReadableInputs;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.StagePartition;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * Reads slices of type {@link StageInputSlice}.
 */
public class StageInputSliceReader implements InputSliceReader
{
  private final String queryId;
  private final InputChannels inputChannels;

  public StageInputSliceReader(String queryId, InputChannels inputChannels)
  {
    this.queryId = queryId;
    this.inputChannels = inputChannels;
  }

  @Override
  public int numReadableInputs(final InputSlice slice)
  {
    final StageInputSlice stageInputSlice = (StageInputSlice) slice;
    return Iterables.size(stageInputSlice.getPartitions());
  }

  @Override
  public ReadableInputs attach(
      final int inputNumber,
      final InputSlice slice,
      final CounterTracker counters,
      final Consumer<Throwable> warningPublisher
  )
  {
    final StageInputSlice stageInputSlice = (StageInputSlice) slice;
    final StageId stageId = new StageId(queryId, stageInputSlice.getStageNumber());
    final FrameReader frameReader = inputChannels.frameReader(stageInputSlice.getStageNumber());

    return ReadableInputs.channels(
        () -> Iterators.transform(
            stageInputSlice.getPartitions().iterator(),
            partition -> {
              final StagePartition stagePartition = new StagePartition(stageId, partition.getPartitionNumber());

              try {
                return ReadableInput.channel(
                    new CountingReadableFrameChannel(
                        inputChannels.openChannel(stagePartition),
                        counters.channel(CounterNames.inputChannel(inputNumber)),
                        stagePartition.getPartitionNumber()
                    ),
                    frameReader,
                    stagePartition
                );
              }
              catch (IOException e) {
                throw new RuntimeException(e);
              }
            }
        ),
        frameReader
    );
  }
}
