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

package org.apache.druid.indexing.common;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class ParseExceptionMetadata
{
  private final String taskId;
  private final String groupId;
  private final String dataSource;

  public ParseExceptionMetadata(String taskId, String groupId, String dataSource)
  {
    this.taskId = taskId;
    this.groupId = groupId;
    this.dataSource = dataSource;
  }

  public String getTaskId()
  {
    return taskId;
  }

  public String getGroupId()
  {
    return groupId;
  }

  public String getDataSource()
  {
    return dataSource;
  }

  public Map<String, Object> toMap()
  {
    return ImmutableMap.of("taskId", taskId, "groupId", groupId, "dataSource", dataSource);
  }
}
