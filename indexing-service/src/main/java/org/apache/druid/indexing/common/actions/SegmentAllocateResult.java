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

package org.apache.druid.indexing.common.actions;

import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;

public class SegmentAllocateResult
{
  private final SegmentIdWithShardSpec segmentId;
  private final String errorMessage;

  public SegmentAllocateResult(SegmentIdWithShardSpec segmentId, String errorMessage)
  {
    this.segmentId = segmentId;
    this.errorMessage = errorMessage;
  }

  public SegmentIdWithShardSpec getSegmentId()
  {
    return segmentId;
  }

  public String getErrorMessage()
  {
    return errorMessage;
  }

  public boolean isSuccess()
  {
    return segmentId != null;
  }
}
