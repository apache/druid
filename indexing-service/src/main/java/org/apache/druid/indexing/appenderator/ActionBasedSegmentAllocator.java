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

package org.apache.druid.indexing.appenderator;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.realtime.appenderator.SegmentAllocator;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;

import javax.annotation.Nullable;
import java.io.IOException;

public class ActionBasedSegmentAllocator implements SegmentAllocator
{
  private final TaskActionClient taskActionClient;
  private final DataSchema dataSchema;
  private final SegmentAllocateActionGenerator actionGenerator;

  public ActionBasedSegmentAllocator(
      TaskActionClient taskActionClient,
      DataSchema dataSchema,
      SegmentAllocateActionGenerator actionGenerator
  )
  {
    this.taskActionClient = taskActionClient;
    this.dataSchema = dataSchema;
    this.actionGenerator = actionGenerator;
  }

  @Nullable
  @Override
  public SegmentIdWithShardSpec allocate(
      final InputRow row,
      final String sequenceName,
      final String previousSegmentId,
      final boolean skipSegmentLineageCheck
  ) throws IOException
  {
    return taskActionClient.submit(
        actionGenerator.generate(dataSchema, row, sequenceName, previousSegmentId, skipSegmentLineageCheck)
    );
  }
}
