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

import java.util.Objects;

/**
 * Message for {@link ControllerClient#postWorkerError}.
 */
public class WorkerError implements ControllerMessage
{
  private final String queryId;
  private final MSQErrorReport errorWrapper;

  @JsonCreator
  public WorkerError(
      @JsonProperty("queryId") String queryId,
      @JsonProperty("error") MSQErrorReport errorWrapper
  )
  {
    this.queryId = Preconditions.checkNotNull(queryId, "queryId");
    this.errorWrapper = Preconditions.checkNotNull(errorWrapper, "error");
  }

  @Override
  @JsonProperty
  public String getQueryId()
  {
    return queryId;
  }

  @JsonProperty("error")
  public MSQErrorReport getErrorWrapper()
  {
    return errorWrapper;
  }

  @Override
  public void handle(Controller controller)
  {
    controller.workerError(errorWrapper);
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
    WorkerError that = (WorkerError) o;
    return Objects.equals(queryId, that.queryId)
           && Objects.equals(errorWrapper, that.errorWrapper);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(queryId, errorWrapper);
  }

  @Override
  public String toString()
  {
    return "WorkerError{" +
           "queryId='" + queryId + '\'' +
           ", errorWrapper=" + errorWrapper +
           '}';
  }
}
