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

package org.apache.druid.segment.loading;

import org.apache.druid.timeline.DataSegment;

import java.io.IOException;

/**
 * A {@link DataSegmentKiller} that does nothing, but will throw a {@link LoadingSegment}
 */
public class FailingDataSegmentKiller implements DataSegmentKiller
{
  private final DataSegment failingSegment;

  public FailingDataSegmentKiller(DataSegment failingSegment)
  {
    this.failingSegment = failingSegment;
  }

  @Override
  public void kill(DataSegment segment) throws SegmentLoadingException
  {
    if (failingSegment.equals(segment)) {
      throw new SegmentLoadingException("Thrown from FailingDataSegmentKiller#kill %s", failingSegment);
    }
  }

  @Override
  public void killAll() throws IOException
  {
    throw new IOException("Thrown from test - FailingDataSegmentKiller#killAll");
  }
}
