/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.java.util.emitter.core;

import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


final class EmittedBatchCounter
{
  /**
   * Max {@link Phaser}'s phase, specified in it's javadoc. Then it wraps to zero.
   */
  private static final int MAX_PHASE = Integer.MAX_VALUE;

  static int nextBatchNumber(int prevBatchNumber)
  {
    return (prevBatchNumber + 1) & MAX_PHASE;
  }

  private final Phaser phaser = new Phaser(1);

  void batchEmitted()
  {
    phaser.arrive();
  }

  void awaitBatchEmitted(int batchNumberToAwait, long timeout, TimeUnit unit)
      throws InterruptedException, TimeoutException
  {
    batchNumberToAwait &= MAX_PHASE;
    int currentBatch = phaser.getPhase();
    checkNotTerminated(currentBatch);
    while (comparePhases(batchNumberToAwait, currentBatch) >= 0) {
      currentBatch = phaser.awaitAdvanceInterruptibly(currentBatch, timeout, unit);
      checkNotTerminated(currentBatch);
    }
  }

  private static int comparePhases(int phase1, int phase2)
  {
    int diff = (phase1 - phase2) & MAX_PHASE;
    if (diff == 0) {
      return 0;
    }
    return diff < MAX_PHASE / 2 ? 1 : -1;
  }

  private void checkNotTerminated(int phase)
  {
    if (phase < 0) {
      throw new IllegalStateException("Phaser[" + phaser + "] unexpectedly terminated.");
    }
  }
}
