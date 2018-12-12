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

package org.apache.druid.collections;

import javax.annotation.Nullable;
import java.util.List;

public interface BlockingPool<T>
{
  /**
   * Returns the total pool size.
   */
  int maxSize();

  /**
   * Poll all available resources from the pool. If there's no available resource, it returns an empty list.
   */
  List<ReferenceCountingResourceHolder<T>> pollAll();

  /**
   * Take a resource from the pool, waiting up to the
   * specified wait time if necessary for an element to become available.
   *
   * @param timeoutMs maximum time to wait for a resource, in milliseconds.
   *
   * @return a resource, or null if the timeout was reached
   */
  @Nullable
  ReferenceCountingResourceHolder<T> take(long timeoutMs);

  /**
   * Take a resource from the pool, waiting if necessary until an element becomes available.
   *
   * @return a resource
   */
  ReferenceCountingResourceHolder<T> take();

  /**
   * Take resources from the pool, waiting up to the
   * specified wait time if necessary for elements of the given number to become available.
   *
   * @param elementNum number of resources to take
   * @param timeoutMs  maximum time to wait for resources, in milliseconds.
   *
   * @return {@link TakeBatchResult} containing a list of resource holders if it succeeds. An empty list should be
   *         returned if there aren't enough available resources. The result also contains the number of remaining
   *         resources after this call no matter it succeeded or not.
   */
  TakeBatchResult<T> takeBatch(int elementNum, long timeoutMs);

  /**
   * Take resources from the pool, waiting if necessary until the elements of the given number become available.
   *
   * @param elementNum number of resources to take
   *
   * @return {@link TakeBatchResult} containing a list of resource holders and the number of remaining resources.
   */
  TakeBatchResult<T> takeBatch(int elementNum);

  class TakeBatchResult<T>
  {
    private final List<ReferenceCountingResourceHolder<T>> elements;
    private final int numAvailableElements;

    public TakeBatchResult(List<ReferenceCountingResourceHolder<T>> elements, int numAvailableElements)
    {
      this.elements = elements;
      this.numAvailableElements = numAvailableElements;
    }

    public boolean isOk()
    {
      return elements.size() > 0;
    }

    public List<ReferenceCountingResourceHolder<T>> getElements()
    {
      return elements;
    }

    public int getNumAvailableElements()
    {
      return numAvailableElements;
    }
  }
}
