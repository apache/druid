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

package io.druid.query;

import io.druid.java.util.common.guava.Sequence;

import java.util.Map;

/**
 * This interface has two similar run() methods. {@link #run(Query, Map)} is legacy and {@link #run(QueryPlus, Map)}
 * is the new one. Their default implementations delegate to each other. Every implementation of QueryRunner should
 * override only one of those methods. New implementations should override the new method: {@link #run(QueryPlus, Map)}.
 */
public interface QueryRunner<T>
{
  /**
   * @deprecated use and override {@link #run(QueryPlus, Map)} instead. This method is going to be removed in Druid 0.11
   */
  @Deprecated
  default Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
  {
    return run(QueryPlus.wrap(query), responseContext);
  }

  /**
   * Runs the given query and returns results in a time-ordered sequence.
   */
  default Sequence<T> run(QueryPlus<T> queryPlus, Map<String, Object> responseContext)
  {
    return run(queryPlus.getQuery(), responseContext);
  }
}
