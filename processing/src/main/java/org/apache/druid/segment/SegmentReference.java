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

import org.apache.druid.query.SegmentDescriptor;

import java.util.Optional;

/**
 * Wrapper for a {@link SegmentDescriptor} and {@link Optional<Segment>}, the latter being created by a
 * {@link SegmentMapFunction} being applied to a {@link ReferenceCountedSegmentProvider}.
 */
public class SegmentReference
{
  private final SegmentDescriptor segmentDescriptor;
  private final Optional<Segment> segmentReference;

  public SegmentReference(SegmentDescriptor segmentDescriptor, Optional<Segment> segmentReference)
  {
    this.segmentDescriptor = segmentDescriptor;
    this.segmentReference = segmentReference;
  }

  public SegmentDescriptor getSegmentDescriptor()
  {
    return segmentDescriptor;
  }

  public Optional<Segment> getSegmentReference()
  {
    return segmentReference;
  }
}
