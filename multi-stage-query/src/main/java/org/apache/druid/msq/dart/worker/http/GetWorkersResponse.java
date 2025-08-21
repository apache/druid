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

package org.apache.druid.msq.dart.worker.http;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Objects;

/**
 * Response from {@link DartWorkerResource#httpGetWorkers(HttpServletRequest)}, the "get all workers" API.
 */
public class GetWorkersResponse
{
  private final List<DartWorkerInfo> workers;

  public GetWorkersResponse(@JsonProperty("workers") final List<DartWorkerInfo> workers)
  {
    this.workers = workers;
  }

  @JsonProperty
  public List<DartWorkerInfo> getWorkers()
  {
    return workers;
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
    GetWorkersResponse that = (GetWorkersResponse) o;
    return Objects.equals(workers, that.workers);
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(workers);
  }
}
