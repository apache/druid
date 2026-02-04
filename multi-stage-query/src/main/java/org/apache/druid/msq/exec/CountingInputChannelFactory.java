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

import com.google.common.base.Preconditions;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.msq.counters.CounterNames;
import org.apache.druid.msq.counters.CounterTracker;
import org.apache.druid.msq.indexing.CountingReadableFrameChannel;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.stage.StageInputSlice;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.WorkOrder;

import java.io.IOException;
import java.util.List;

/**
 * Wrapper around {@link InputChannelFactory} that increments counters as data is read.
 */
public class CountingInputChannelFactory implements InputChannelFactory
{
  private final InputChannelFactory baseFactory;
  private final WorkOrder workOrder;
  private final CounterTracker counterTracker;

  public CountingInputChannelFactory(
      InputChannelFactory baseFactory,
      WorkOrder workOrder,
      CounterTracker counterTracker
  )
  {
    this.baseFactory = Preconditions.checkNotNull(baseFactory, "baseFactory");
    this.workOrder = Preconditions.checkNotNull(workOrder, "workOrder");
    this.counterTracker = Preconditions.checkNotNull(counterTracker, "counterTracker");
  }

  @Override
  public ReadableFrameChannel openChannel(StageId stageId, int workerNumber, int partitionNumber) throws IOException
  {
    return new CountingReadableFrameChannel(
        baseFactory.openChannel(stageId, workerNumber, partitionNumber),
        counterTracker.channel(getCounterNameForStage(stageId.getStageNumber())),
        partitionNumber
    );
  }

  /**
   * Returns the counter name based on the input number (position in {@link WorkOrder#getInputs()}) for a stage.
   * If multiple match, or if none matches, uses the size of the input array (which ends up not corresponding to
   * any input number, allowing detection of these situations when looking at counters).
   */
  private String getCounterNameForStage(int stageNumber)
  {
    final List<InputSlice> inputs = workOrder.getInputs();
    Integer matchingSlice = null;
    for (int i = 0; i < inputs.size(); i++) {
      final InputSlice slice = inputs.get(i);
      if (slice instanceof StageInputSlice && ((StageInputSlice) slice).getStageNumber() == stageNumber) {
        if (matchingSlice == null) {
          matchingSlice = i;
        } else {
          matchingSlice = null;
          break;
        }
      }
    }

    return CounterNames.inputChannel(matchingSlice == null ? inputs.size() : matchingSlice);
  }
}
