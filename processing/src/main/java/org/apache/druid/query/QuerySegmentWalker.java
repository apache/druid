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

import org.joda.time.Interval;

/**
 */
public interface QuerySegmentWalker
{
  /**
   * Gets the Queryable for a given interval, the Queryable returned can be any version(s) or partitionNumber(s)
   * such that it represents the interval.
   *
   * @param <T> query result type
   * @param query the query to find a Queryable for
   * @param intervals the intervals to find a Queryable for
   * @return a Queryable object that represents the interval
   */
  <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals);

  /**
   * Gets the Queryable for a given list of SegmentSpecs.
   *
   * @param <T> the query result type
   * @param query the query to return a Queryable for
   * @param specs the list of SegmentSpecs to find a Queryable for
   * @return the Queryable object with the given SegmentSpecs
   */
  <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs);
}
