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

package org.apache.druid.java.util.common;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class BlockingQueueHelper<T>
{
  /**
   * A wrapper method around BlockingQueue#offer that provides users with the capability of adding a method to
   * handle a failure to offer.
   */
  public boolean offerAndHandleFailure (
      BlockingQueue<T> queue,
      T itemToOffer,
      long waitTime,
      TimeUnit waitTimeUnit,
      FailureHandler handler) throws InterruptedException
  {
    boolean successful = queue.offer(itemToOffer, waitTime, waitTimeUnit);
    if (!successful) {
      handler.handleFailure();
    }
    return successful;
  }

  public interface FailureHandler
  {
    void handleFailure();
  }
}
