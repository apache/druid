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

package org.apache.druid.timeline;

import org.apache.druid.guice.annotations.PublicApi;
import org.joda.time.Interval;

/**
 * A logical segment can represent an entire segment or a part of a segment. As a result, it can have a different
 * interval from its actual base segment. {@link #getInterval()} and {@link #getTrueInterval()} return the interval of
 * this logical segment and the interval of the base segment, respectively.
 *
 * For example, suppose we have 2 segments as below:
 *
 * - Segment A has an interval of 2017/2018.
 * - Segment B has an interval of 2017-08-01/2017-08-02.
 *
 * For these segments, {@link VersionedIntervalTimeline#lookup} returns 3 segments as below:
 *
 * - interval of 2017/2017-08-01 (trueInterval: 2017/2018)
 * - interval of 2017-08-01/2017-08-02 (trueInterval: 2017-08-01/2017-08-02)
 * - interval of 2017-08-02/2018 (trueInterval: 2017/2018)
 */
@PublicApi
public interface LogicalSegment
{
  Interval getInterval();

  Interval getTrueInterval();
}
