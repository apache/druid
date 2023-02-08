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

import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Collections;

/**
 * A {@link SegmentWrangler} for {@link InlineDataSource}.
 *
 * It is not valid to pass any other DataSource type to the "getSegmentsForIntervals" method.
 */
public class InlineSegmentWrangler implements SegmentWrangler
{
  private static final String SEGMENT_ID = "inline";

  @Override
  @SuppressWarnings("unchecked")
  public Iterable<Segment> getSegmentsForIntervals(final DataSource dataSource, final Iterable<Interval> intervals)
  {
    final InlineDataSource inlineDataSource = (InlineDataSource) dataSource;

    if (inlineDataSource.rowsAreArrayList()) {
      return Collections.singletonList(
          new ArrayListSegment<>(
              SegmentId.dummy(SEGMENT_ID),
              (ArrayList<Object[]>) inlineDataSource.getRowsAsList(),
              inlineDataSource.rowAdapter(),
              inlineDataSource.getRowSignature()
          )
      );
    }

    return Collections.singletonList(
        new RowBasedSegment<>(
            SegmentId.dummy(SEGMENT_ID),
            Sequences.simple(inlineDataSource.getRows()),
            inlineDataSource.rowAdapter(),
            inlineDataSource.getRowSignature()
        )
    );
  }
}
