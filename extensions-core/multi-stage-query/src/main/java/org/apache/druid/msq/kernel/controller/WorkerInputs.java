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

package org.apache.druid.msq.kernel.controller;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.input.InputSpecSlicer;
import org.apache.druid.msq.input.NilInputSlice;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.WorkerAssignmentStrategy;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

/**
 * Represents assignment of {@link InputSlice} to workers.
 */
public class WorkerInputs
{
  // Worker number -> input number -> input slice.
  private final Int2ObjectMap<List<InputSlice>> assignmentsMap;

  private WorkerInputs(final Int2ObjectMap<List<InputSlice>> assignmentsMap)
  {
    this.assignmentsMap = assignmentsMap;
  }

  /**
   * Create worker assignments for a stage.
   */
  public static WorkerInputs create(
      final StageDefinition stageDef,
      final Int2IntMap stageWorkerCountMap,
      final InputSpecSlicer slicer,
      final WorkerAssignmentStrategy assignmentStrategy
  )
  {
    // Split each inputSpec and assign to workers. This list maps worker number -> input number -> input slice.
    final Int2ObjectMap<List<InputSlice>> assignmentsMap = new Int2ObjectAVLTreeMap<>();
    final int numInputs = stageDef.getInputSpecs().size();

    if (numInputs == 0) {
      // No inputs: run a single worker. (It might generate some data out of nowhere.)
      assignmentsMap.put(0, Collections.singletonList(NilInputSlice.INSTANCE));
      return new WorkerInputs(assignmentsMap);
    }

    // Assign input slices to workers.
    for (int inputNumber = 0; inputNumber < numInputs; inputNumber++) {
      final InputSpec inputSpec = stageDef.getInputSpecs().get(inputNumber);

      if (stageDef.getBroadcastInputNumbers().contains(inputNumber)) {
        // Broadcast case: send everything everywhere.
        final List<InputSlice> broadcastSlices = slicer.sliceStatic(inputSpec, 1);
        final InputSlice broadcastSlice = broadcastSlices.isEmpty()
                                          ? NilInputSlice.INSTANCE
                                          : Iterables.getOnlyElement(broadcastSlices);

        for (int workerNumber = 0; workerNumber < stageDef.getMaxWorkerCount(); workerNumber++) {
          assignmentsMap.computeIfAbsent(
              workerNumber,
              ignored -> Arrays.asList(new InputSlice[numInputs])
          ).set(inputNumber, broadcastSlice);
        }
      } else {
        // Non-broadcast case: split slices across workers.
        final List<InputSlice> slices = assignmentStrategy.assign(stageDef, inputSpec, stageWorkerCountMap, slicer);

        // Flip the slices, so it's worker number -> slices for that worker.
        for (int workerNumber = 0; workerNumber < slices.size(); workerNumber++) {
          assignmentsMap.computeIfAbsent(
              workerNumber,
              ignored -> Arrays.asList(new InputSlice[numInputs])
          ).set(inputNumber, slices.get(workerNumber));
        }
      }
    }

    final ObjectIterator<Int2ObjectMap.Entry<List<InputSlice>>> assignmentsIterator =
        assignmentsMap.int2ObjectEntrySet().iterator();

    boolean first = true;
    while (assignmentsIterator.hasNext()) {
      final Int2ObjectMap.Entry<List<InputSlice>> entry = assignmentsIterator.next();
      final List<InputSlice> slices = entry.getValue();

      // Replace all null slices with nil slices: this way, logic later on doesn't have to deal with nulls.
      for (int inputNumber = 0; inputNumber < numInputs; inputNumber++) {
        if (slices.get(inputNumber) == null) {
          slices.set(inputNumber, NilInputSlice.INSTANCE);
        }
      }

      // Eliminate workers that have no non-nil, non-broadcast inputs. (Except the first one, because if all input
      // is nil, *some* worker has to do *something*.)
      final boolean hasNonNilNonBroadcastInput =
          IntStream.range(0, numInputs)
                   .anyMatch(i ->
                                 !slices.get(i).equals(NilInputSlice.INSTANCE)  // Non-nil
                                 && !stageDef.getBroadcastInputNumbers().contains(i) // Non-broadcast
                   );

      if (!first && !hasNonNilNonBroadcastInput) {
        assignmentsIterator.remove();
      }

      first = false;
    }

    return new WorkerInputs(assignmentsMap);
  }

  public List<InputSlice> inputsForWorker(final int workerNumber)
  {
    return Preconditions.checkNotNull(assignmentsMap.get(workerNumber), "worker [%s]", workerNumber);
  }

  public IntSet workers()
  {
    return assignmentsMap.keySet();
  }

  public int workerCount()
  {
    return assignmentsMap.size();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WorkerInputs that = (WorkerInputs) o;
    return Objects.equals(assignmentsMap, that.assignmentsMap);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(assignmentsMap);
  }

  @Override
  public String toString()
  {
    return assignmentsMap.toString();
  }
}
