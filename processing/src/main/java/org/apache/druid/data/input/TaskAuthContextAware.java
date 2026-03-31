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

package org.apache.druid.data.input;

import org.apache.druid.auth.TaskAuthContext;

import javax.annotation.Nullable;

/**
 * Interface for InputSources that need access to task authentication context
 * (e.g., for authenticating with external catalogs like Iceberg REST Catalog).
 *
 * <p>When an InputSource implements this interface, the task execution framework
 * will call {@link #setTaskAuthContext(TaskAuthContext)} before the input source
 * is used, passing the authentication context from the task.
 *
 * <p>This allows input sources to access user credentials for operations like:
 * <ul>
 *   <li>Authenticating with Iceberg REST Catalog for OAuth token-based access</li>
 *   <li>Vending temporary S3 credentials from the catalog</li>
 *   <li>Other external service authentication scenarios</li>
 * </ul>
 *
 * @see TaskAuthContext
 */
public interface TaskAuthContextAware
{
  /**
   * Sets the task authentication context for this input source.
   * Called by the task execution framework before the input source is used.
   *
   * @param taskAuthContext the auth context containing credentials, may be null
   *                        if no auth context is available for the task
   */
  void setTaskAuthContext(@Nullable TaskAuthContext taskAuthContext);
}
