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

package org.apache.druid.msq.input;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.frame.channel.ReadableNilFrameChannel;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.counters.CounterTracker;
import org.apache.druid.msq.input.stage.ReadablePartitions;
import org.apache.druid.msq.input.stage.StageInputSlice;
import org.apache.druid.msq.kernel.StagePartition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class InputSlices
{
  private InputSlices()
  {
    // No instantiation.
  }

  /**
   * Combines all {@link StageInputSlice#getPartitions()} from the input slices that are {@link StageInputSlice}.
   * Ignores other types of input slices.
   */
  public static ReadablePartitions allReadablePartitions(final List<InputSlice> slices)
  {
    final List<ReadablePartitions> partitionsList = new ArrayList<>();

    for (final InputSlice slice : slices) {
      if (slice instanceof StageInputSlice) {
        partitionsList.add(((StageInputSlice) slice).getPartitions());
      }
    }

    return ReadablePartitions.combine(partitionsList);
  }

  /**
   * Sum of {@link InputSliceReader#numReadableInputs(InputSlice)} across all input slices that are _not_ present
   * in "broadcastInputs".
   */
  public static int getNumNonBroadcastReadableInputs(
      final List<InputSlice> slices,
      final InputSliceReader reader,
      final IntSet broadcastInputs
  )
  {
    int numInputs = 0;

    for (int i = 0; i < slices.size(); i++) {
      if (!broadcastInputs.contains(i)) {
        numInputs += reader.numReadableInputs(slices.get(i));
      }
    }

    return numInputs;
  }

  /**
   * Calls {@link InputSliceReader#attach} on all "slices", which must all be {@link NilInputSlice} or
   * {@link StageInputSlice}, and collects like-numbered partitions.
   *
   * The returned map is keyed by partition number. Each value is a list of inputs of the
   * same length as "slices", and in the same order. i.e., the first ReadableInput in each list corresponds to the
   * first provided {@link InputSlice}.
   *
   * "Missing" partitions -- which occur when one slice has no data for a given partition -- are replaced with
   * {@link ReadableInput} based on {@link ReadableNilFrameChannel}, with no {@link StagePartition}.
   *
   * @throws IllegalStateException if any slices are not {@link StageInputSlice}
   */
  public static Int2ObjectMap<List<ReadableInput>> attachAndCollectPartitions(
      final List<InputSlice> slices,
      final InputSliceReader reader,
      final CounterTracker counters,
      final Consumer<Throwable> warningPublisher
  )
  {
    // Input number -> ReadableInputs.
    final List<ReadableInputs> inputsByInputNumber = new ArrayList<>();

    for (final InputSlice slice : slices) {
      if (slice instanceof NilInputSlice) {
        inputsByInputNumber.add(null);
      } else if (slice instanceof StageInputSlice) {
        final ReadableInputs inputs = reader.attach(inputsByInputNumber.size(), slice, counters, warningPublisher);
        inputsByInputNumber.add(inputs);
      } else {
        throw new ISE("Slice [%s] is not a 'stage' slice", slice);
      }
    }

    // Populate the result map.
    final Int2ObjectMap<List<ReadableInput>> retVal = new Int2ObjectRBTreeMap<>();

    for (int inputNumber = 0; inputNumber < slices.size(); inputNumber++) {
      for (final ReadableInput input : inputsByInputNumber.get(inputNumber)) {
        if (input != null) {
          final int partitionNumber = input.getStagePartition().getPartitionNumber();
          retVal.computeIfAbsent(partitionNumber, ignored -> Arrays.asList(new ReadableInput[slices.size()]))
                .set(inputNumber, input);
        }
      }
    }

    // Fill in all nulls with NilInputSlice.
    for (Int2ObjectMap.Entry<List<ReadableInput>> entry : retVal.int2ObjectEntrySet()) {
      for (int inputNumber = 0; inputNumber < entry.getValue().size(); inputNumber++) {
        if (entry.getValue().get(inputNumber) == null) {
          entry.getValue().set(
              inputNumber,
              ReadableInput.channel(
                  ReadableNilFrameChannel.INSTANCE,
                  inputsByInputNumber.get(inputNumber).frameReader(),
                  null
              )
          );
        }
      }
    }

    return retVal;
  }
}
