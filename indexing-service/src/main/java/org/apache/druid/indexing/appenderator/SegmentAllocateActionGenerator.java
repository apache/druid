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
import org.apache.druid.indexing.common.actions.TaskAction;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;

/**
 * This class is used in {@link ActionBasedSegmentAllocator} and expected to generate a
 * {@link TaskAction<  SegmentIdWithShardSpec  >} which is submitted to overlords to allocate a new segment.
 * The {@link #generate} method can return any implementation of {@link TaskAction<  SegmentIdWithShardSpec  >}.
 *
 * @see org.apache.druid.indexing.common.actions.SegmentAllocateAction
 * @see org.apache.druid.indexing.common.actions.SurrogateAction
 */
public interface SegmentAllocateActionGenerator
{
  TaskAction<SegmentIdWithShardSpec> generate(
      DataSchema dataSchema,
      InputRow row,
      String sequenceName,
      String previousSegmentId,
      boolean skipSegmentLineageCheck
  );
}
