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

import org.apache.druid.auth.TaskAuthContext;
import org.apache.druid.guice.annotations.ExtensionPoint;
import org.apache.druid.server.security.AuthenticationResult;

import javax.annotation.Nullable;

/**
 * Creates {@link TaskAuthContext} from authentication results during task submission.
 *
 * <p>Implementations extract relevant credentials from the {@link AuthenticationResult}
 * (populated by the Authenticator) and create a TaskAuthContext that will be passed
 * to the task for use during execution.
 *
 * <p>This interface is bound optionally via Guice. If no implementation is configured,
 * no auth context will be injected into tasks.
 *
 * @see TaskAuthContext
 */
@ExtensionPoint
public interface TaskAuthContextProvider
{
  /**
   * Extract auth context from the authentication result for the given task.
   *
   * @param authenticationResult the authentication result from the Authenticator,
   *                             containing identity and context map with credentials
   * @param task                 the task being submitted, can be used to make decisions
   *                             based on task type, datasource, etc.
   * @return TaskAuthContext to inject into the task, or null to skip injection
   */
  @Nullable
  TaskAuthContext createTaskAuthContext(AuthenticationResult authenticationResult, Task task);
}
