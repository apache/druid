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

package io.druid.segment.realtime.appenderator;

import io.druid.timeline.DataSegment;

import java.io.IOException;
import java.util.Set;

public interface UsedSegmentChecker
{
  /**
   * For any identifiers that exist and are actually used, returns the corresponding DataSegment objects.
   *
   * @param identifiers identifiers to search for
   *
   * @return used DataSegments
   */
  Set<DataSegment> findUsedSegments(Set<SegmentIdentifier> identifiers) throws IOException;
}
