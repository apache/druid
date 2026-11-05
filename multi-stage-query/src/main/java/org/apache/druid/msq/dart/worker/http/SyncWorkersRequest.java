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

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import java.util.Set;

/**
 * Request to {@link DartWorkerResource#httpPostSync(SyncWorkersRequest, HttpServletRequest)}.
 */
public record SyncWorkersRequest(@JsonProperty("queryIds") Set<String> queryIds)
{
  public SyncWorkersRequest(@JsonProperty("queryIds") @Nullable final Set<String> queryIds)
  {
    this.queryIds = queryIds == null ? Set.of() : queryIds;
  }
}
