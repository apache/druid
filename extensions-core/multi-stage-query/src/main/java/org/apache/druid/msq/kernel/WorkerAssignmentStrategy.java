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

package org.apache.druid.msq.kernel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.exec.Limits;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.input.InputSpecSlicer;

import java.util.List;
import java.util.OptionalInt;

/**
 * Strategy for assigning input slices to tasks. Influences how {@link InputSpecSlicer} is used.
 */
public enum WorkerAssignmentStrategy
{
  /**
   * Use the highest possible number of tasks, while staying within {@link StageDefinition#getMaxWorkerCount()}.
   *
   * Implemented using {@link InputSpecSlicer#sliceStatic}.
   */
  MAX {
    @Override
    public List<InputSlice> assign(
        StageDefinition stageDef,
        InputSpec inputSpec,
        Int2IntMap stageWorkerCountMap,
        InputSpecSlicer slicer
    )
    {
      return slicer.sliceStatic(inputSpec, stageDef.getMaxWorkerCount());
    }
  },

  /**
   * Use the lowest possible number of tasks, while keeping each task's workload under
   * {@link Limits#MAX_INPUT_FILES_PER_WORKER} files and {@link Limits#MAX_INPUT_BYTES_PER_WORKER} bytes.
   *
   * Implemented using {@link InputSpecSlicer#sliceDynamic} whenever possible.
   */
  AUTO {
    @Override
    public List<InputSlice> assign(
        final StageDefinition stageDef,
        final InputSpec inputSpec,
        final Int2IntMap stageWorkerCountMap,
        final InputSpecSlicer slicer
    )
    {
      if (slicer.canSliceDynamic(inputSpec)) {
        return slicer.sliceDynamic(
            inputSpec,
            stageDef.getMaxWorkerCount(),
            Limits.MAX_INPUT_FILES_PER_WORKER,
            Limits.MAX_INPUT_BYTES_PER_WORKER
        );
      } else {
        // In auto mode, if we can't slice inputs dynamically, we instead carry forwards the number of workers from
        // the prior stages (or use 1 worker if there are no input stages).

        // To handle cases where the input stage is limited to 1 worker because it is reading 1 giant file, I think it
        // would be better to base the number of workers on the number of rows read by the prior stage, which would
        // allow later stages to fan out when appropriate. However, we're not currently tracking this information
        // in a way that is accessible to the assignment strategy.

        final IntSet inputStages = stageDef.getInputStageNumbers();
        final OptionalInt maxInputStageWorkerCount = inputStages.intStream().map(stageWorkerCountMap).max();
        final int workerCount = maxInputStageWorkerCount.orElse(1);
        return slicer.sliceStatic(inputSpec, workerCount);
      }
    }
  };

  @JsonCreator
  public static WorkerAssignmentStrategy fromString(final String name)
  {
    if (name == null) {
      throw new NullPointerException("Null worker assignment strategy");
    }
    return valueOf(StringUtils.toUpperCase(name));
  }

  @Override
  @JsonValue
  public String toString()
  {
    return StringUtils.toLowerCase(name());
  }

  public abstract List<InputSlice> assign(
      StageDefinition stageDef,
      InputSpec inputSpec,
      Int2IntMap stageWorkerCountMap,
      InputSpecSlicer slicer
  );
}
