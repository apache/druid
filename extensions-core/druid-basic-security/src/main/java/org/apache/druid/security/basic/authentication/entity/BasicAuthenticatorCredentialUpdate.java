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

package org.apache.druid.security.basic.authentication.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

public class BasicAuthenticatorCredentialUpdate
{
  private final String password;
  private final int iterations;

  @JsonCreator
  public BasicAuthenticatorCredentialUpdate(
      @JsonProperty("password") String password,
      @JsonProperty("iterations") Integer iterations
  )
  {
    Preconditions.checkNotNull(password, "Cannot assign null password.");
    Preconditions.checkArgument(!StringUtils.isEmpty(password), "Cannot assign empty password.");
    this.password = password;
    this.iterations = iterations == null ? -1 : iterations;
  }

  @JsonProperty
  public String getPassword()
  {
    return password;
  }

  @JsonProperty
  public int getIterations()
  {
    return iterations;
  }
}
