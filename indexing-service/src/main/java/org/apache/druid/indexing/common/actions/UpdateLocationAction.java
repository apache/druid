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

package org.apache.druid.indexing.common.actions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexing.common.task.Task;

/* This class was added for mm-less ingestion in order to let the peon manage its own location lifecycle by submitting
actions to the overlord. https://github.com/apache/druid/pull/15133 moved this location logic to the overlord itself
so this Action is no longer needed. For backwards compatibility with old peons, this class was left in but can be deprecated
for a later druid release.
*/
@Deprecated
public class UpdateLocationAction implements TaskAction<Void>
{
  @JsonIgnore
  private final TaskLocation taskLocation;

  @JsonCreator
  public UpdateLocationAction(
      @JsonProperty("location") TaskLocation location
  )
  {
    this.taskLocation = location;
  }

  @JsonProperty
  public TaskLocation getTaskLocation()
  {
    return taskLocation;
  }

  @Override
  public TypeReference<Void> getReturnTypeReference()
  {
    return new TypeReference<Void>()
    {
    };
  }

  @Override
  public Void perform(Task task, TaskActionToolbox toolbox)
  {
    return null;
  }

  @Override
  public boolean isAudited()
  {
    return true;
  }

  @Override
  public String toString()
  {
    return "UpdateLocationAction{" +
           "taskLocation=" + taskLocation +
           '}';
  }
}
