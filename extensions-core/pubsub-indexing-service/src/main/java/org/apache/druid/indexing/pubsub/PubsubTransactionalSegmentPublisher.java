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

package org.apache.druid.indexing.pubsub;

import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.realtime.appenderator.TransactionalSegmentPublisher;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Set;

public class PubsubTransactionalSegmentPublisher
    implements TransactionalSegmentPublisher
{
  private final PubsubIndexTaskRunner runner;
  private final TaskToolbox toolbox;
  private final boolean useTransaction;

  public PubsubTransactionalSegmentPublisher(
      PubsubIndexTaskRunner runner,
      TaskToolbox toolbox,
      boolean useTransaction
  )
  {
    this.runner = runner;
    this.toolbox = toolbox;
    this.useTransaction = useTransaction;
  }

  @Override
  public SegmentPublishResult publishAnnotatedSegments(
      @Nullable Set<DataSegment> mustBeNullOrEmptySegments,
      Set<DataSegment> segmentsToPush,
      @Nullable Object commitMetadata
  ) throws IOException
  {
    if (mustBeNullOrEmptySegments != null && !mustBeNullOrEmptySegments.isEmpty()) {
      throw new ISE("WTH? stream ingestion tasks are overwriting segments[%s]", mustBeNullOrEmptySegments);
    }
    final SegmentTransactionalInsertAction action;

    if (segmentsToPush.isEmpty()) {
      return SegmentPublishResult.ok(segmentsToPush);
    } else {
      action = SegmentTransactionalInsertAction.appendAction(segmentsToPush, null, null);
    }

    return toolbox.getTaskActionClient().submit(action);
  }

  @Override
  public boolean supportsEmptyPublish()
  {
    return true;
  }
}
