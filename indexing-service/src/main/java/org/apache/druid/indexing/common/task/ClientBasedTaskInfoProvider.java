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

package org.apache.druid.indexing.common.task;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskInfoProvider;

public class ClientBasedTaskInfoProvider implements TaskInfoProvider
{
  private final IndexingServiceClient client;

  @Inject
  public ClientBasedTaskInfoProvider(IndexingServiceClient client)
  {
    this.client = client;
  }

  @Override
  public TaskLocation getTaskLocation(String id)
  {
    final TaskStatusResponse response = client.getTaskStatus(id);
    return response == null ? TaskLocation.unknown() : response.getStatus().getLocation();
  }

  @Override
  public Optional<TaskStatus> getTaskStatus(String id)
  {
    final TaskStatusResponse response = client.getTaskStatus(id);
    return response == null ?
           Optional.absent() :
           Optional.of(TaskStatus.fromCode(id, response.getStatus().getStatusCode()));
  }
}
