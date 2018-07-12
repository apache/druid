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

package io.druid.indexing.appenderator;

import io.druid.data.input.InputRow;
import io.druid.indexing.common.actions.TaskAction;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;

/**
 * This class is used in {@link ActionBasedSegmentAllocator} and expected to generate a
 * {@link TaskAction<SegmentIdentifier>} which is submitted to overlords to allocate a new segment.
 * The {@link #generate} method can return any implementation of {@link TaskAction<SegmentIdentifier>}.
 *
 * @see io.druid.indexing.common.actions.SegmentAllocateAction
 * @see io.druid.indexing.common.actions.SurrogateAction
 */
public interface SegmentAllocateActionGenerator
{
  TaskAction<SegmentIdentifier> generate(
      DataSchema dataSchema,
      InputRow row,
      String sequenceName,
      String previousSegmentId,
      boolean skipSegmentLineageCheck
  );
}
