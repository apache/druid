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

package org.apache.druid.query;

/**
 * An implementation of {@link PrioritizedCallable} that also lets caller get access to associated {@link QueryRunner}
 * It is used in implementations of {@link QueryRunnerFactory}
 * @param <T> - Type of result of {@link #call()} method
 * @param <V> - Type of {@link org.apache.druid.java.util.common.guava.Sequence} of rows returned by {@link QueryRunner}
 */
public interface PrioritizedQueryRunnerCallable<T, V> extends PrioritizedCallable<T>
{
  /**
   * This method can be used by the extensions to get the runner that the given query execution task corresponds to.
   * That in turn can be used to fetch any state associated with the QueryRunner such as the segment info for example.
   * Extensions can carry any state from custom implementation of QuerySegmentWalker to a
   * custom implementation of {@link QueryProcessingPool#submitRunnerTask(PrioritizedQueryRunnerCallable)}
   */
  QueryRunner<V> getRunner();
}
