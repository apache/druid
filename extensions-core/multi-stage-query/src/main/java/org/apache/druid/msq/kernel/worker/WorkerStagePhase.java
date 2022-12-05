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
      return priorPhase == PRESHUFFLE_WAITING_FOR_RESULT_PARTITION_BOUNDARIES;
    }
  },

  RESULTS_READY {
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
      return priorPhase == RESULTS_READY;
    }
  },

  // Something went wrong.
  FAILED {
    @Override
    public boolean canTransitionFrom(final WorkerStagePhase priorPhase)
    {
      return true;
    }
  };

  public abstract boolean canTransitionFrom(WorkerStagePhase priorPhase);
}
