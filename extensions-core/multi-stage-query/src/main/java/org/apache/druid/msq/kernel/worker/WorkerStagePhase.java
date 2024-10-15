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

package org.apache.druid.msq.kernel.worker;

import org.apache.druid.msq.exec.ProcessingBuffers;

/**
 * Phases that a stage can be in, as far as the worker is concerned.
 *
 * Used by {@link WorkerStageKernel}.
 */
public enum WorkerStagePhase
{
  NEW {
    @Override
    public boolean canTransitionFrom(final WorkerStagePhase priorPhase)
    {
      return false;
    }
  },

  READING_INPUT {
    @Override
    public boolean canTransitionFrom(final WorkerStagePhase priorPhase)
    {
      return priorPhase == NEW;
    }
  },

  PRESHUFFLE_WAITING_FOR_RESULT_PARTITION_BOUNDARIES {
    @Override
    public boolean canTransitionFrom(final WorkerStagePhase priorPhase)
    {
      return priorPhase == READING_INPUT;
    }
  },

  PRESHUFFLE_WRITING_OUTPUT {
    @Override
    public boolean canTransitionFrom(final WorkerStagePhase priorPhase)
    {
      return priorPhase == PRESHUFFLE_WAITING_FOR_RESULT_PARTITION_BOUNDARIES /* if globally sorting */
             || priorPhase == READING_INPUT /* if locally sorting */;
    }
  },

  RESULTS_COMPLETE {
    @Override
    public boolean canTransitionFrom(final WorkerStagePhase priorPhase)
    {
      return priorPhase == READING_INPUT || priorPhase == PRESHUFFLE_WRITING_OUTPUT;
    }
  },

  FINISHED {
    @Override
    public boolean canTransitionFrom(final WorkerStagePhase priorPhase)
    {
      // Stages can transition to FINISHED even if they haven't generated all output yet. For example, this is
      // possible if the downstream stage is applying a limit.
      return priorPhase.compareTo(FINISHED) < 0;
    }
  },

  // Something went wrong.
  FAILED {
    @Override
    public boolean canTransitionFrom(final WorkerStagePhase priorPhase)
    {
      return !priorPhase.isTerminal();
    }
  };

  public abstract boolean canTransitionFrom(WorkerStagePhase priorPhase);

  /**
   * Whether this phase indicates that the stage is no longer running.
   */
  public boolean isTerminal()
  {
    return this == FINISHED || this == FAILED;
  }

  /**
   * Whether this phase indicates a stage is running and consuming its full complement of resources.
   *
   * Importantly, stages that are not running are not holding {@link ProcessingBuffers}.
   *
   * There are still some resources that can be consumed by stages that are not running. For example, in the
   * {@link #FINISHED} state, stages can still have data on disk that has not been cleaned-up yet, some pointers
   * to that data that still reside in memory, and some counters in memory available for collection by the controller.
   */
  public boolean isRunning()
  {
    return this != NEW && this != RESULTS_COMPLETE && this != FINISHED && this != FAILED;
  }
}
