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

package org.apache.druid.sql.calcite.schema;

import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import java.util.Collection;

/**
 * Public interface representing a callback mechanism for changes in metadata segment views.
 */
public interface MetadataSegmentViewCallback
{
  /**
   * Callback method invoked only once when the timeline associated
   * with the metadata segment view is fully initialized.
   */
  void timelineInitialized();

  /**
   * Callback method invoked when new data segments are added to the metadata segment view.
   * Full {@link DataSegment} objects are provided since receivers need them to initialize
   * their own state (e.g. create selectors).
   *
   * @param segments the collection of data segments that have been added
   */
  void segmentsAdded(Collection<DataSegment> segments);

  /**
   * Callback method invoked when data segments are removed from the metadata segment view.
   * Only {@link SegmentId}s are provided — receivers are expected to already hold the full
   * {@link DataSegment} in their own state and can look it up by ID.
   *
   * @param segmentIds the IDs of segments that have been removed
   */
  void segmentsRemoved(Collection<SegmentId> segmentIds);
}
