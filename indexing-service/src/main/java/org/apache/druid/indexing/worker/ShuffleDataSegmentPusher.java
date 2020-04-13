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

package org.apache.druid.indexing.worker;

import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.timeline.DataSegment;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Map;

/**
 * DataSegmentPusher used for storing intermediary data in local storage during data shuffle of native parallel
 * indexing.
 */
public class ShuffleDataSegmentPusher implements DataSegmentPusher
{
  private final String supervisorTaskId;
  private final String subTaskId;
  private final IntermediaryDataManager intermediaryDataManager;

  public ShuffleDataSegmentPusher(
      String supervisorTaskId,
      String subTaskId,
      IntermediaryDataManager intermediaryDataManager
  )
  {
    this.supervisorTaskId = supervisorTaskId;
    this.subTaskId = subTaskId;
    this.intermediaryDataManager = intermediaryDataManager;
  }

  @Override
  public String getPathForHadoop(String dataSource)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getPathForHadoop()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public DataSegment push(File file, DataSegment segment, boolean useUniquePath) throws IOException
  {
    final long unzippedSize = intermediaryDataManager.addSegment(supervisorTaskId, subTaskId, segment, file);
    return segment.withSize(unzippedSize)
                  .withBinaryVersion(SegmentUtils.getVersionFromDir(file));
  }

  @Override
  public Map<String, Object> makeLoadSpec(URI finalIndexZipFilePath)
  {
    throw new UnsupportedOperationException();
  }
}
