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

import com.google.inject.Inject;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.DataSource;
import org.joda.time.Interval;

import java.util.Map;

/**
 * An implementation of {@link SegmentWrangler} that allows registration of DataSource-specific handlers via Guice.
 *
 * @see org.apache.druid.guice.DruidBinders#segmentWranglerBinder  to register wranglers
 */
public class MapSegmentWrangler implements SegmentWrangler
{
  private final Map<Class<? extends DataSource>, SegmentWrangler> wranglers;

  @Inject
  public MapSegmentWrangler(final Map<Class<? extends DataSource>, SegmentWrangler> wranglers)
  {
    this.wranglers = wranglers;
  }

  @Override
  public Iterable<Segment> getSegmentsForIntervals(final DataSource dataSource, final Iterable<Interval> intervals)
  {
    final SegmentWrangler wrangler = wranglers.get(dataSource.getClass());

    if (wrangler != null) {
      return wrangler.getSegmentsForIntervals(dataSource, intervals);
    } else {
      // Reasonable as a user-facing error message.
      throw new ISE("Cannot read directly out of dataSource: %s", dataSource);
    }
  }
}
