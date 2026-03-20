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

package org.apache.druid.msq.dart.controller.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.msq.exec.Controller;
import org.apache.druid.msq.exec.ControllerClient;
import org.apache.druid.msq.indexing.error.MSQErrorReport;

import java.util.List;
import java.util.Objects;

/**
 * Message for {@link ControllerClient#postWorkerWarning}.
 */
public class WorkerWarning implements ControllerMessage
{
  private final String queryId;
  private final List<MSQErrorReport> errorWrappers;

  @JsonCreator
  public WorkerWarning(
      @JsonProperty("queryId") String queryId,
      @JsonProperty("errors") List<MSQErrorReport> errorWrappers
  )
  {
    this.queryId = Preconditions.checkNotNull(queryId, "queryId");
    this.errorWrappers = Preconditions.checkNotNull(errorWrappers, "error");
  }

  @Override
  @JsonProperty
  public String getQueryId()
  {
    return queryId;
  }

  @JsonProperty("errors")
  public List<MSQErrorReport> getErrorWrappers()
  {
    return errorWrappers;
  }

  @Override
  public void handle(Controller controller)
  {
    controller.workerWarning(errorWrappers);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WorkerWarning that = (WorkerWarning) o;
    return Objects.equals(queryId, that.queryId) && Objects.equals(errorWrappers, that.errorWrappers);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(queryId, errorWrappers);
  }

  @Override
  public String toString()
  {
    return "WorkerWarning{" +
           "queryId='" + queryId + '\'' +
           ", errorWrappers=" + errorWrappers +
           '}';
  }
}
