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

import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * Phases that a stage can be in, as far as the controller is concerned.
 *
 * Used by {@link ControllerStageTracker}.
 */
public enum ControllerStagePhase
{
  // Not doing anything yet. Just recently initialized.
  NEW {
    @Override
    public boolean canTransitionFrom(final ControllerStagePhase priorPhase)
    {
      return false;
    }
  },

  // Reading and mapping inputs (using "stateless" operators like filters, transforms which operate on individual records).
  READING_INPUT {
    @Override
    public boolean canTransitionFrom(final ControllerStagePhase priorPhase)
    {
      return priorPhase == NEW;
    }
  },

  // Post the inputs have been read and mapped to frames, in the `POST_READING` stage, we pre-shuffle and determing the partition boundaries.
  // This step for a stage spits out the statistics of the data as a whole (and not just the individual records). This
  // phase is not required in non-pre shuffle contexts.
  POST_READING {
    @Override
    public boolean canTransitionFrom(final ControllerStagePhase priorPhase)
    {
      return priorPhase == READING_INPUT;
    }
  },

  // Done doing work and all results have been generated.
  RESULTS_READY {
    @Override
    public boolean canTransitionFrom(final ControllerStagePhase priorPhase)
    {
      return priorPhase == READING_INPUT || priorPhase == POST_READING;
    }
  },

  // The worker outputs for this stage might have been cleaned up in the workers, and they cannot be used by
  // any other phase. "Metadata" for the stage such as counters are still available however
  FINISHED {
    @Override
    public boolean canTransitionFrom(final ControllerStagePhase priorPhase)
    {
      return priorPhase == RESULTS_READY;
    }
  },

  // Something went wrong.
  FAILED {
    @Override
    public boolean canTransitionFrom(final ControllerStagePhase priorPhase)
    {
      return true;
    }
  };

  public abstract boolean canTransitionFrom(ControllerStagePhase priorPhase);

  private static final Set<ControllerStagePhase> TERMINAL_PHASES = ImmutableSet.of(
      RESULTS_READY,
      FINISHED
  );

  /**
   * @return true if the phase indicates that the stage has completed its work and produced results successfully
   */
  public static boolean isSuccessfulTerminalPhase(final ControllerStagePhase phase)
  {
    return TERMINAL_PHASES.contains(phase);
  }

  private static final Set<ControllerStagePhase> POST_READING_PHASES = ImmutableSet.of(
      POST_READING,
      RESULTS_READY,
      FINISHED
  );

  /**
   * @return true if the phase indicates that the stage has consumed its inputs from the previous stages successfully
   */
  public static boolean isPostReadingPhase(final ControllerStagePhase phase)
  {
    return POST_READING_PHASES.contains(phase);
  }
}
