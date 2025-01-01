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

package org.apache.druid.msq.shuffle.input;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.exec.OutputChannelMode;
import org.apache.druid.msq.indexing.InputChannelFactory;
import org.apache.druid.msq.input.stage.StageInputSlice;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.WorkOrder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Meta-factory that wraps {@link #inputChannelFactoryProvider}, and can create various other kinds of factories.
 */
public class MetaInputChannelFactory implements InputChannelFactory
{
  private final Int2ObjectMap<OutputChannelMode> stageOutputModeMap;
  private final Function<OutputChannelMode, InputChannelFactory> inputChannelFactoryProvider;
  private final Map<OutputChannelMode, InputChannelFactory> inputChannelFactoryMap = new HashMap<>();

  public MetaInputChannelFactory(
      final Int2ObjectMap<OutputChannelMode> stageOutputModeMap,
      final Function<OutputChannelMode, InputChannelFactory> inputChannelFactoryProvider
  )
  {
    this.stageOutputModeMap = stageOutputModeMap;
    this.inputChannelFactoryProvider = inputChannelFactoryProvider;
  }

  /**
   * Create a meta-factory.
   *
   * @param slices                      stage slices from {@link WorkOrder#getInputs()}
   * @param defaultOutputChannelMode    mode to use when {@link StageInputSlice#getOutputChannelMode()} is null; i.e.,
   *                                    when running with an older controller
   * @param inputChannelFactoryProvider provider of {@link InputChannelFactory} for various {@link OutputChannelMode}
   */
  public static MetaInputChannelFactory create(
      final List<StageInputSlice> slices,
      final OutputChannelMode defaultOutputChannelMode,
      final Function<OutputChannelMode, InputChannelFactory> inputChannelFactoryProvider
  )
  {
    final Int2ObjectMap<OutputChannelMode> stageOutputModeMap = new Int2ObjectOpenHashMap<>();

    for (final StageInputSlice slice : slices) {
      final OutputChannelMode newMode;

      if (slice.getOutputChannelMode() != null) {
        newMode = slice.getOutputChannelMode();
      } else {
        newMode = defaultOutputChannelMode;
      }

      final OutputChannelMode prevMode = stageOutputModeMap.putIfAbsent(
          slice.getStageNumber(),
          newMode
      );

      if (prevMode != null && prevMode != newMode) {
        throw new ISE(
            "Inconsistent output modes for stage[%s], got[%s] and[%s]",
            slice.getStageNumber(),
            prevMode,
            newMode
        );
      }
    }

    return new MetaInputChannelFactory(stageOutputModeMap, inputChannelFactoryProvider);
  }

  @Override
  public ReadableFrameChannel openChannel(
      final StageId stageId,
      final int workerNumber,
      final int partitionNumber
  ) throws IOException
  {
    final OutputChannelMode outputChannelMode = stageOutputModeMap.get(stageId.getStageNumber());

    if (outputChannelMode == null) {
      throw new ISE("No output mode for stageNumber[%s]", stageId.getStageNumber());
    }

    return inputChannelFactoryMap.computeIfAbsent(outputChannelMode, inputChannelFactoryProvider)
                                 .openChannel(stageId, workerNumber, partitionNumber);
  }
}
