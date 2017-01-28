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

package io.druid.query.monomorphicprocessing;

import javax.annotation.Nullable;

/**
 * @param <T> type of query processing algorithm
 * @see SpecializationService
 */
public abstract class SpecializationState<T>
{
  /**
   * Returns an instance of specialized version of query processing algorithm, if available, null otherwise.
   */
  @Nullable
  public abstract T getSpecialized();

  /**
   * Returns an instance of specialized version of query processing algorithm, if available, defaultInstance otherwise.
   */
  public final T getSpecializedOrDefault(T defaultInstance)
  {
    T specialized = getSpecialized();
    return specialized != null ? specialized : defaultInstance;
  }

  /**
   * Accounts the number of loop iterations, made when processing queries without specialized algorithm, i. e. after
   * {@link #getSpecialized()} returned null. If sufficiently many loop iterations were made, {@link
   * SpecializationService} decides that the algorithm is worth to be specialized, and {@link #getSpecialized()} will
   * return non-null during later queries.
   */
  public abstract void accountLoopIterations(long loopIterations);
}
