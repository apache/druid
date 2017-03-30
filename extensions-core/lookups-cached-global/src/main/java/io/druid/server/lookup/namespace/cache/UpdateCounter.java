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

package io.druid.server.lookup.namespace.cache;

import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

final class UpdateCounter
{
  /**
   * Max {@link Phaser}'s phase, specified in it's javadoc. Then it wraps to zero.
   */
  private static final int MAX_PHASE = Integer.MAX_VALUE;

  private final Phaser phaser = new Phaser(1);

  void update()
  {
    phaser.arrive();
  }

  void awaitTotalUpdates(int totalUpdates) throws InterruptedException
  {
    totalUpdates &= MAX_PHASE;
    int currentUpdates = phaser.getPhase();
    checkNotTerminated(currentUpdates);
    while (comparePhases(totalUpdates, currentUpdates) > 0) {
      currentUpdates = phaser.awaitAdvanceInterruptibly(currentUpdates);
      checkNotTerminated(currentUpdates);
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

  void awaitNextUpdates(int nextUpdates) throws InterruptedException
  {
    if (nextUpdates <= 0) {
      throw new IllegalArgumentException("nextUpdates is not positive: " + nextUpdates);
    }
    if (nextUpdates > MAX_PHASE / 4) {
      throw new UnsupportedOperationException("Couldn't wait for so many updates: " + nextUpdates);
    }
    awaitTotalUpdates(phaser.getPhase() + nextUpdates);
  }

  boolean awaitFirstUpdate(long timeout, TimeUnit unit) throws InterruptedException
  {
    try {
      phaser.awaitAdvanceInterruptibly(0, timeout, unit);
      return true;
    }
    catch (TimeoutException e) {
      return false;
    }
  }
}
