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

package io.druid.server.coordinator;

import io.druid.timeline.DataSegment;

import java.util.concurrent.ConcurrentSkipListSet;

public class LoadQueuePeonTester extends LoadQueuePeon
{
  private final ConcurrentSkipListSet<DataSegment> segmentsToLoad = new ConcurrentSkipListSet<DataSegment>();

  public LoadQueuePeonTester()
  {
    super(null, null, null, null, null, null);
  }

  @Override
  public void loadSegment(
      DataSegment segment,
      LoadPeonCallback callback
  )
  {
    segmentsToLoad.add(segment);
  }

  @Override
  public ConcurrentSkipListSet<DataSegment> getSegmentsToLoad()
  {
    return segmentsToLoad;
  }

  @Override
  public int getNumberOfSegmentsInQueue()
  {
    return segmentsToLoad.size();
  }
}
