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

package org.apache.druid.indexing.template;

import org.apache.druid.client.indexing.ClientTaskQuery;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.query.http.ClientSqlQuery;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * A batch indexing job that can be launched by the Overlord as a task.
 * A job may contain the {@link ClientTaskQuery} itself or an MSQ query that gets converted
 * by the Broker to a {@code ControllerTask} and is then submitted to the Overlord.
 */
public class BatchIndexingJob
{
  private final boolean isMsq;
  private final ClientSqlQuery msqQuery;
  private final ClientTaskQuery task;

  protected BatchIndexingJob(
      @Nullable ClientTaskQuery task,
      @Nullable ClientSqlQuery msqQuery
  )
  {
    this.isMsq = task == null;
    this.msqQuery = msqQuery;
    this.task = task;

    InvalidInput.conditionalException(
        (task == null || msqQuery == null) && (task != null || msqQuery != null),
        "Exactly one of 'task' or 'msqQuery' must be non-null"
    );
  }

  /**
   * @return MSQ query to be run in this job, if any.
   * @throws NullPointerException if this not an MSQ job.
   */
  public ClientSqlQuery getNonNullMsqQuery()
  {
    return Objects.requireNonNull(msqQuery);
  }

  /**
   * @return Task to be run in this job, if any.
   * @throws NullPointerException if this is an MSQ job.
   */
  public ClientTaskQuery getNonNullTask()
  {
    return Objects.requireNonNull(task);
  }

  /**
   * @return true if this is an MSQ job.
   */
  public boolean isMsq()
  {
    return isMsq;
  }

  @Override
  public String toString()
  {
    return "BatchIndexingJob{" +
           "isMsq=" + isMsq +
           ", msqQuery=" + msqQuery +
           ", task=" + task +
           '}';
  }
}
