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

package org.apache.druid.segment;

import org.apache.druid.query.DataSource;
import org.joda.time.Interval;

/**
 * Utility for creating {@link Segment} objects for concrete datasources.
 *
 * @see org.apache.druid.guice.DruidBinders#segmentWranglerBinder  to register factories
 */
public interface SegmentWrangler
{
  /**
   * Gets Segments for a particular datasource and set of intervals. These are expected to exist for any datasource
   * where {@link DataSource#isConcrete} and {@link DataSource#isGlobal} are both true (corresponding to datasources
   * where any Druid server could scan its data).
   *
   * Note: there are no SegmentWranglers for 'table' datasources (Druid's distributed datasources) because those are
   * special and handled in their own special way.
   *
   * @return Segments that, collectively, contain data for dataSource. May be empty if dataSource does not exist or
   * has no data in the provided intervals. May contain data outside the provided intervals, so callers should
   * filter it down further, e.g. through the "interval" parameter of {@link StorageAdapter#makeCursors}
   */
  Iterable<Segment> getSegmentsForIntervals(DataSource dataSource, Iterable<Interval> intervals);
}
