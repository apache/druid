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

package org.apache.druid.indexing.common.task.batch.parallel;

import org.apache.druid.indexing.common.task.AbstractBatchIndexTask;
import org.apache.druid.indexing.common.task.TaskResource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

public abstract class AbstractBatchSubtask extends AbstractBatchIndexTask
{

  protected AbstractBatchSubtask(
      String id,
      @Nullable String groupId,
      @Nullable TaskResource taskResource,
      String dataSource,
      @Nullable Map<String, Object> context,
      @Nonnull IngestionMode ingestionMode
  )
  {
    super(id, groupId, taskResource, dataSource, context, -1, ingestionMode);
  }

  /**
   * Returns the ID of {@link SubTaskSpec} that is assigned to this subtask.
   * This ID is used to identify duplicate work of retry tasks for the same spec.
   */
  public abstract String getSubtaskSpecId();
}
