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

package org.apache.druid.server.system.table;

import org.apache.druid.query.SystemTableAccess;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;

/**
 * Decides which rows of a system table a caller may read.
 *
 * <p>The returned access is applied to the provider's rows after filters on pushdown columns, and node scans apply
 * their filters before the Broker applies it. A table whose access hides rows must therefore not reveal them through
 * those filters, for example through an error from evaluating a filter on a hidden value.</p>
 */
public interface SystemTableRowAuthorizer
{
  /**
   * Returns the rows of the table that the given identity may read.
   *
   * @throws ForbiddenException if the identity may not read the table at all
   */
  SystemTableAccess authorize(AuthenticationResult authenticationResult, AuthorizerMapper authorizerMapper);
}
