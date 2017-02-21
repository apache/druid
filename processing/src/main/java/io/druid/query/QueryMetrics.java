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

import com.metamx.emitter.service.ServiceEmitter;

import java.util.Map;

/**
 * Abstraction wrapping {@link com.metamx.emitter.service.ServiceMetricEvent.Builder} and allowing to control what
 * metrics are actually emitted, what dimensions do they have, etc.
 *
 *
 * Goals of QueryMetrics
 * ---------------------
 *  1. Skipping or partial filtering of particular dimensions or metrics entirely. Implementation could leave the body
 *  of the corresponding empty, or implement random filtering like:
 *  public void cpuTime(ServiceEmitter emitter, long timeNs)
 *  {
 *    if (ThreadLocalRandom.current().nextDouble() < 0.1) {
 *      super.cpuTime(emitter, timeNs);
 *    }
 *  }
 *
 *  2. Ability to add new dimensions and metrics, possibly expensive to compute, or expensive to process (long string
 *  values, high cardinality, etc.) and not to affect existing Druid installations, by skipping (see 1.) those
 *  dimensions and metrics entirely in the default QueryMetrics implementations. Users who need those expensive
 *  dimensions and metrics, could explicitly emit them in their own QueryMetrics.
 *
 *  3. Control over the time unit, in which time metrics are emitted. By default (see {@link DefaultQueryMetrics} and
 *  it's subclasses) it's milliseconds, but if queries are fast, it could be not precise enough.
 *
 *  4. Control over the dimension and metric names.
 *
 *
 * Types of methods in this interface
 * ----------------------------------
 *  1. Methods, pulling some dimensions from the query object. These methods are used to populate the metric before the
 *  query is run. These methods accept a single `QueryType query` parameter.
 *  2. Methods for general dimensions, which become known in the process of the query execution or after the query is
 *  completed.
 *  3. Methods for emitting metrics, they accept {@link ServiceEmitter} as the first parameter.
 *
 *
 * Implementors expectations
 * -------------------------
 * QueryMetrics is expected to be changed often, in every Druid release. Users who create their custom implementations
 * of QueryMetrics should be ready to fix the code of their QueryMetrics (implement new methods) when they update Druid.
 * Broken builds of custom extensions, containing custom QueryMetrics is the way to notify users that Druid core "wants"
 * to emit new dimension or metric, and the user handles them manually: if the new dimension or metric is useful and not
 * very expensive to process and store then emit, skip (see above Goals, 1.) otherwise.
 *
 *
 * Adding new methods to QueryMetrics
 * ----------------------------------
 * 1. When adding a new method for emitting a dimension, which could be pulled from the query object, always make them
 * accept a single `QueryType query` parameter, letting the implementations to do all the work of carving the dimension
 * value out of the query object.
 *
 * 2. When adding a new method for emitting a dimension, which becomes known in the process of the query execution or
 * after the query is completed, design them so that as little work as possible is done for preparing arguments for this
 * method, and as much work as possible is done in the implementations of this method, if they decide to emit this
 * dimension.
 *
 * 3. When adding a new method for emitting metrics, make it to accept the metric value in the smallest reasonable
 * unit (i. e. nanoseconds for time metrics, bytes for metrics of data size, etc.), allowing the implementations of
 * this method to round the value up to more coarse-grained units, if they don't need the maximum precision.
 *
 * @param <QueryType>
 */
public interface QueryMetrics<QueryType extends Query<?>>
{
  /**
   * Pulls all information for the query object into dimensions of future metrics events.
   */
  void query(QueryType query);

  /**
   * Sets {@link Query#getDataSource()} of the given query as dimension.
   */
  void dataSource(QueryType query);

  /**
   * Sets {@link Query#getType()} of the given query as dimension.
   */
  void queryType(QueryType query);

  /**
   * Sets {@link Query#getIntervals()} of the given query as dimension.
   */
  void interval(QueryType query);

  /**
   * Sets {@link Query#hasFilters()} of the given query as dimension.
   */
  void hasFilters(QueryType query);

  /**
   * Sets {@link Query#getDuration()} of the given query as dimension.
   */
  void duration(QueryType query);

  /**
   * Sets {@link Query#getId()} of the given query as dimension.
   */
  void queryId(QueryType query);

  /**
   * Sets {@link Query#getContext()} of the given query as dimension.
   */
  void context(QueryType query);

  void server(String host);

  void remoteAddress(String removeAddress);

  void userDimensions(Map<String, String> userDimensions);

  void status(String status);

  void success(boolean success);

  void queryTime(ServiceEmitter emitter, long timeNs);

  void queryBytes(ServiceEmitter emitter, long byteCount);

  void waitTime(ServiceEmitter emitter, long timeNs);

  void segmentTime(ServiceEmitter emitter, long timeNs);

  void segmentAndCacheTime(ServiceEmitter emitter, long timeNs);

  void intervalChunkTime(ServiceEmitter emitter, long timeNs);

  void cpuTime(ServiceEmitter emitter, long timeNs);

  void nodeTimeToFirstByte(ServiceEmitter emitter, long timeNs);

  void nodeTime(ServiceEmitter emitter, long timeNs);

  void nodeBytes(ServiceEmitter emitter, long byteCount);
}
