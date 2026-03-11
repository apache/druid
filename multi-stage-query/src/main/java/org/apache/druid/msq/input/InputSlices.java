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

import org.apache.druid.msq.input.stage.ReadablePartitions;
import org.apache.druid.msq.input.stage.StageInputSlice;

import java.util.ArrayList;
import java.util.List;

public class InputSlices
{
  private InputSlices()
  {
    // No instantiation.
  }

  /**
   * Returns all {@link StageInputSlice} from the provided list of input slices. Ignores other types of input slices.
   */
  public static List<StageInputSlice> allStageSlices(final List<InputSlice> slices)
  {
    final List<StageInputSlice> retVal = new ArrayList<>();

    for (final InputSlice slice : slices) {
      if (slice instanceof StageInputSlice) {
        retVal.add((StageInputSlice) slice);
      }
    }

    return retVal;
  }

  /**
   * Combines all {@link StageInputSlice#getPartitions()} from the input slices that are {@link StageInputSlice}.
   * Ignores other types of input slices.
   */
  public static ReadablePartitions allReadablePartitions(final List<InputSlice> slices)
  {
    final List<ReadablePartitions> partitionsList = new ArrayList<>();

    for (final StageInputSlice slice : allStageSlices(slices)) {
      partitionsList.add(slice.getPartitions());
    }

    return ReadablePartitions.combine(partitionsList);
  }
}
