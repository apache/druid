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

package org.apache.druid.curator.discovery;

import org.apache.druid.server.DruidNode;

import javax.annotation.Nullable;
import java.util.concurrent.CountDownLatch;

/**
 * A test service announcer that counts down the corresponding latches upon
 * invocation of {@link #announce(DruidNode)} and {@link #unannounce(DruidNode)}.
 */
public class LatchableServiceAnnouncer implements ServiceAnnouncer
{
  private final CountDownLatch announceLatch;
  private final CountDownLatch unannounceLatch;

  /**
   * Creates a new {@link LatchableServiceAnnouncer} with the given countdown
   * latches for announce and unannounce actions.
   */
  public LatchableServiceAnnouncer(
      @Nullable CountDownLatch announceLatch,
      @Nullable CountDownLatch unannounceLatch
  )
  {
    this.announceLatch = announceLatch;
    this.unannounceLatch = unannounceLatch;
  }

  @Override
  public void announce(DruidNode node)
  {
    if (announceLatch != null) {
      announceLatch.countDown();
    }
  }

  @Override
  public void unannounce(DruidNode node)
  {
    if (unannounceLatch != null) {
      unannounceLatch.countDown();
    }
  }
}
