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

import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.segment.FrameSegment;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.FrameBasedInlineDataSource;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

public class FrameBasedInlineSegmentWrangler implements SegmentWrangler
{

  private static final String SEGMENT_ID = "inline";

  @Override
  public Iterable<Segment> getSegmentsForIntervals(
      DataSource dataSource,
      Iterable<Interval> intervals
  )
  {
    final FrameBasedInlineDataSource frameBasedInlineDataSource = (FrameBasedInlineDataSource) dataSource;

    return () -> frameBasedInlineDataSource
        .getFrames()
        .stream()
        .<Segment>map(
            frameSignaturePair -> new FrameSegment(
                frameSignaturePair.getFrame(),
                FrameReader.create(frameSignaturePair.getRowSignature()),
                SegmentId.dummy(SEGMENT_ID)
            )
        )
        .iterator();

  }
}
