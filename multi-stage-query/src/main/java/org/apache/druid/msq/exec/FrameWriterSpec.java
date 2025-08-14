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

package org.apache.druid.msq.exec;

import com.google.common.base.Preconditions;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.write.FrameWriters;
import org.apache.druid.msq.kernel.WorkOrder;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.QueryContext;

/**
 * Contains the query-wide items that need to be passed to {@link FrameWriters#makeFrameWriterFactory}.
 */
public class FrameWriterSpec
{
  private final FrameType rowBasedFrameType;
  private final boolean removeNullBytes;

  public FrameWriterSpec(
      final FrameType rowBasedFrameType,
      final boolean removeNullBytes
  )
  {
    this.rowBasedFrameType = Preconditions.checkNotNull(rowBasedFrameType, "rowBasedFrameType");
    this.removeNullBytes = removeNullBytes;
  }

  /**
   * Create an instance from query context or {@link WorkOrder#getWorkerContext()}.
   */
  public static FrameWriterSpec fromContext(final QueryContext queryContext)
  {
    return new FrameWriterSpec(
        MultiStageQueryContext.getRowBasedFrameType(queryContext),
        MultiStageQueryContext.removeNullBytes(queryContext)
    );
  }

  /**
   * Row-based frame type, generally taken from {@link MultiStageQueryContext#getRowBasedFrameType(QueryContext)}.
   */
  public FrameType getRowBasedFrameType()
  {
    return rowBasedFrameType;
  }

  /**
   * Whether frame writers should remove null bytes, generally taken from
   * {@link MultiStageQueryContext#removeNullBytes(QueryContext)}.
   */
  public boolean getRemoveNullBytes()
  {
    return removeNullBytes;
  }
}
