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
  private final Phaser phaser = new Phaser(1);

  void update()
  {
    phaser.arrive();
  }

  void awaitTotalUpdates(int totalUpdates) throws InterruptedException
  {
    int currentUpdates = phaser.getPhase();
    while (totalUpdates - currentUpdates > 0) { // overflow-aware
      currentUpdates = phaser.awaitAdvanceInterruptibly(currentUpdates);
    }
  }

  void awaitNextUpdates(int nextUpdates) throws InterruptedException
  {
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
