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

import io.druid.data.input.InputRow;

import java.io.IOException;

public interface SegmentAllocator
{
  /**
   * Allocates a new segment for a given timestamp.
   *
   * @param row               the event which triggered this allocation request
   * @param sequenceName      sequenceName for this allocation
   * @param previousSegmentId segment identifier returned on the previous call to allocate for your sequenceName
   *
   * @return the pending segment identifier, or null if it was impossible to allocate a new segment
   */
  SegmentIdentifier allocate(
      InputRow row,
      String sequenceName,
      String previousSegmentId,
      boolean skipSegmentLineageCheck
  ) throws IOException;
}
