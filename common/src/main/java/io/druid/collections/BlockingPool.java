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

package io.druid.collections;

import javax.annotation.Nullable;
import java.util.List;

public interface BlockingPool<T>
{
  int maxSize();

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
   * @return a list of resource holders. An empty list is returned if {@code elementNum} resources aren't available.
   */
  List<ReferenceCountingResourceHolder<T>> takeBatch(int elementNum, long timeoutMs);

  /**
   * Take resources from the pool, waiting if necessary until the elements of the given number become available.
   *
   * @param elementNum number of resources to take
   *
   * @return a list of resource holders. An empty list is returned if {@code elementNum} resources aren't available.
   */
  List<ReferenceCountingResourceHolder<T>> takeBatch(int elementNum);
}
