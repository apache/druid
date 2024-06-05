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

import org.apache.druid.msq.exec.ClusterStatisticsMergeMode;
import org.apache.druid.msq.exec.OutputChannelMode;
import org.apache.druid.msq.kernel.ShuffleKind;
import org.apache.druid.msq.kernel.ShuffleSpec;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.statistics.PartialKeyStatisticsInformation;

/**
 * Phases that a stage can be in, as far as the controller is concerned.
 * <p>
 * Used by {@link ControllerStageTracker}.
 */
public enum ControllerStagePhase
{
  /**
   * Not doing anything yet. Just recently initialized.
   *
   * When using {@link OutputChannelMode#MEMORY}, entering this phase tells us that it is time to launch the consumer
   * stage (see {@link ControllerQueryKernel#readyToReadResults}).
   */
  NEW {
    @Override
    public boolean canTransitionFrom(final ControllerStagePhase priorPhase)
    {
      return false;
    }
  },

  /**
   * Reading inputs.
   *
   * Stages may transition directly from here to {@link #RESULTS_READY}, or they may go through
   * {@link #MERGING_STATISTICS} and {@link #POST_READING}, depending on the {@link ShuffleSpec}.
   */
  READING_INPUT {
    @Override
    public boolean canTransitionFrom(final ControllerStagePhase priorPhase)
    {
      return priorPhase == RETRYING || priorPhase == NEW;
    }
  },

  /**
   * Waiting to fetch key statistics in the background from the workers and incrementally generate partitions.
   *
   * This phase is only transitioned to once all {@link PartialKeyStatisticsInformation} are received from workers.
   * Transitioning to this phase should also enqueue the task to fetch key statistics if
   * {@link ClusterStatisticsMergeMode#SEQUENTIAL} strategy is used. In {@link ClusterStatisticsMergeMode#PARALLEL}
   * strategy, we start fetching the key statistics as soon as they are available on the worker.
   *
   * This stage is used if, and only if, {@link StageDefinition#mustGatherResultKeyStatistics()}.
   */
  MERGING_STATISTICS {
    @Override
    public boolean canTransitionFrom(final ControllerStagePhase priorPhase)
    {
      return priorPhase == READING_INPUT;
    }
  },

  /**
   * Inputs have been completely read, and sorting is in progress.
   *
   * When using {@link OutputChannelMode#MEMORY} with {@link StageDefinition#doesSortDuringShuffle()}, entering this
   * phase tells us that it is time to launch the consumer stage (see {@link ControllerQueryKernel#readyToReadResults}).
   *
   * This phase is only used when {@link ShuffleKind#isSort()}. Note that it may not *always* be used even when sorting;
   * for example, when not using {@link OutputChannelMode#MEMORY} and also not gathering statistics
   * ({@link StageDefinition#mustGatherResultKeyStatistics()}), a stage phase may transition directly from
   * {@link #READING_INPUT} to {@link #RESULTS_READY}.
   */
  POST_READING {
    @Override
    public boolean canTransitionFrom(final ControllerStagePhase priorPhase)
    {
      return priorPhase == READING_INPUT /* when sorting locally */
             || priorPhase == MERGING_STATISTICS /* when sorting globally */;
    }
  },

  /**
   * Done doing work, and all results have been generated.
   */
  RESULTS_READY {
    @Override
    public boolean canTransitionFrom(final ControllerStagePhase priorPhase)
    {
      return priorPhase == READING_INPUT || priorPhase == POST_READING;
    }
  },

  /**
   * Stage has completed successfully and has been cleaned up. Worker outputs for this stage are no longer
   * available and cannot be used by any other stage. Metadata such as counters are still available.
   *
   * Any non-terminal phase can transition to FINISHED. This can even happen prior to RESULTS_READY, if the
   * controller determines that the outputs of the stage are no longer needed. For example, this happens when
   * a downstream consumer is reading with limit, and decides it's finished processing.
   */
  FINISHED {
    @Override
    public boolean canTransitionFrom(final ControllerStagePhase priorPhase)
    {
      return !priorPhase.isTerminal();
    }
  },

  /**
   * Something went wrong.
   */
  FAILED {
    @Override
    public boolean canTransitionFrom(final ControllerStagePhase priorPhase)
    {
      return true;
    }
  },

  /**
   * Stages whose workers are currently under relaunch. We can transition out of this phase only when all the work
   * orders of this stage have been sent. We can transition into this phase when the prior phase did not
   * publish its final results yet.
   */
  RETRYING {
    @Override
    public boolean canTransitionFrom(final ControllerStagePhase priorPhase)
    {
      return priorPhase == READING_INPUT
             || priorPhase == POST_READING
             || priorPhase == MERGING_STATISTICS
             || priorPhase == RETRYING;
    }
  };

  public abstract boolean canTransitionFrom(ControllerStagePhase priorPhase);

  /**
   * Whether this phase indicates that the stage has been started and is still running. (It hasn't been cleaned up
   * or failed yet.)
   */
  public boolean isRunning()
  {
    return this == READING_INPUT
           || this == MERGING_STATISTICS
           || this == POST_READING
           || this == RESULTS_READY
           || this == RETRYING;
  }

  /**
   * Whether this phase indicates that the stage has consumed its inputs from the previous stages successfully.
   */
  public boolean isDoneReadingInput()
  {
    return this == POST_READING || this == RESULTS_READY || this == FINISHED;
  }

  /**
   * Whether this phase indicates that the stage has completed its work and produced results successfully.
   */
  public boolean isSuccess()
  {
    return this == RESULTS_READY || this == FINISHED;
  }

  /**
   * Whether this phase indicates that the stage is no longer running.
   */
  public boolean isTerminal()
  {
    return this == FINISHED || this == FAILED;
  }
}
