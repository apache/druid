/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.security.basic.db;

public class BasicAuthDBConfig
{
  private final String dbPrefix;
  private final String initialAdminPassword;
  private final String initialInternalClientPassword;

  public BasicAuthDBConfig(
      final String dbPrefix,
      final String initialAdminPassword,
      final String initialInternalClientPassword
  )
  {
    this.dbPrefix = dbPrefix;
    this.initialAdminPassword = initialAdminPassword;
    this.initialInternalClientPassword = initialInternalClientPassword;
  }

  public String getDbPrefix()
  {
    return dbPrefix;
  }

  public String getInitialAdminPassword()
  {
    return initialAdminPassword;
  }

  public String getInitialInternalClientPassword()
  {
    return initialInternalClientPassword;
  }
}
