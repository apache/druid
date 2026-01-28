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

package org.apache.druid.indexing.compact;

import org.apache.druid.client.indexing.ClientCompactionTaskQuery;
import org.apache.druid.indexing.template.BatchIndexingJob;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.server.compaction.CompactionCandidate;
import org.apache.druid.timeline.CompactionState;

/**
 * {@link BatchIndexingJob} to compact an interval of a datasource.
 */
public class CompactionJob extends BatchIndexingJob
{
  private final CompactionCandidate candidate;
  private final int maxRequiredTaskSlots;
  private final String targetIndexingStateFingerprint;
  private final CompactionState targetIndexingState;

  public CompactionJob(
      ClientCompactionTaskQuery task,
      CompactionCandidate candidate,
      int maxRequiredTaskSlots,
      String targetIndexingStateFingerprint,
      CompactionState targetIndexingState
  )
  {
    super(task, null);
    this.candidate = candidate;
    this.maxRequiredTaskSlots = maxRequiredTaskSlots;
    this.targetIndexingStateFingerprint = targetIndexingStateFingerprint;
    this.targetIndexingState = targetIndexingState;
  }

  public CompactionJob(
      ClientSqlQuery msqQuery,
      CompactionCandidate candidate,
      int maxRequiredTaskSlots,
      String targetIndexingStateFingerprint,
      CompactionState targetIndexingState
  )
  {
    super(null, msqQuery);
    this.candidate = candidate;
    this.maxRequiredTaskSlots = maxRequiredTaskSlots;
    this.targetIndexingStateFingerprint = targetIndexingStateFingerprint;
    this.targetIndexingState = targetIndexingState;
  }

  public String getDataSource()
  {
    return candidate.getDataSource();
  }

  public CompactionCandidate getCandidate()
  {
    return candidate;
  }

  public int getMaxRequiredTaskSlots()
  {
    return maxRequiredTaskSlots;
  }

  public String getTargetIndexingStateFingerprint()
  {
    return targetIndexingStateFingerprint;
  }

  public CompactionState getTargetIndexingState()
  {
    return targetIndexingState;
  }

  @Override
  public String toString()
  {
    return "CompactionJob{" +
           super.toString() +
           ", candidate=" + candidate +
           ", maxRequiredTaskSlots=" + maxRequiredTaskSlots +
           ", targetIndexingStateFingerprint='" + targetIndexingStateFingerprint + '\'' +
           ", targetIndexingState=" + targetIndexingState +
           '}';
  }
}
