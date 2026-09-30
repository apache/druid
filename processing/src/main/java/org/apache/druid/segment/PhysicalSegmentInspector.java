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

import javax.annotation.Nullable;

/**
 * Interface for methods describing physical segments such as {@link QueryableIndexSegment} and
 * {@link IncrementalIndexSegment} that is not typically used at query time (outside of metadata queries).
 *
 * @deprecated this interface has been split into smaller, single-purpose interfaces. Use {@link RowCountInspector} for
 * the number of rows in a segment, {@link PhysicalSegmentColumnInspector} for column details, and {@link Segment#as}
 * to get {@link Metadata} instead. It is retained, implemented only by {@link QueryableIndexSegment} and
 * {@link IncrementalIndexSegment}, so that existing callers reaching it through {@link Segment#as} continue to work.
 */
@Deprecated
public interface PhysicalSegmentInspector extends RowCountInspector, PhysicalSegmentColumnInspector
{
  /**
   * Returns {@link Metadata} which contains details about how the segment was created
   *
   * @deprecated use {@link Segment#as} to get {@link Metadata} instead
   */
  @Deprecated
  @Nullable
  Metadata getMetadata();
}
