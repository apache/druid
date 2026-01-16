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

package org.apache.druid.msq.test;

import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.test.utils.TestSegmentManager;
import org.apache.druid.timeline.DataSegment;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Map;

/**
 * Data Segment pusher which records generated segments in {@link TestSegmentManager}.
 */
public class MSQTestDelegateDataSegmentPusher implements DataSegmentPusher
{
  private final DataSegmentPusher delegate;
  private final TestSegmentManager testSegmentManager;

  public MSQTestDelegateDataSegmentPusher(
      DataSegmentPusher dataSegmentPusher,
      TestSegmentManager testSegmentManager
  )
  {
    this.delegate = dataSegmentPusher;
    this.testSegmentManager = testSegmentManager;
  }

  @Override
  public String getPathForHadoop(String dataSource)
  {
    return delegate.getPathForHadoop(dataSource);
  }

  @Override
  public String getPathForHadoop()
  {
    return delegate.getPathForHadoop();
  }

  @Override
  public DataSegment push(File file, DataSegment segment, boolean useUniquePath) throws IOException
  {
    final DataSegment dataSegment = delegate.push(file, segment, useUniquePath);
    testSegmentManager.recordGeneratedSegment(dataSegment);
    return dataSegment;
  }

  @Override
  public Map<String, Object> makeLoadSpec(URI finalIndexZipFilePath)
  {
    return delegate.makeLoadSpec(finalIndexZipFilePath);
  }
}
