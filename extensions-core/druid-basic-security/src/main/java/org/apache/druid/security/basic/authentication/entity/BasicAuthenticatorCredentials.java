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
import org.apache.druid.security.basic.BasicAuthUtils;
import org.apache.druid.security.basic.authentication.validator.PasswordHashGenerator;

import java.util.Arrays;

public class BasicAuthenticatorCredentials
{
  private final byte[] salt;
  private final byte[] hash;
  private final int iterations;

  @JsonCreator
  public BasicAuthenticatorCredentials(
      @JsonProperty("salt") byte[] salt,
      @JsonProperty("hash") byte[] hash,
      @JsonProperty("iterations") int iterations
  )
  {
    Preconditions.checkNotNull(salt);
    Preconditions.checkNotNull(hash);
    this.salt = salt;
    this.hash = hash;
    this.iterations = iterations;
  }

  public BasicAuthenticatorCredentials(BasicAuthenticatorCredentialUpdate update)
  {
    this.iterations = update.getIterations();
    this.salt = BasicAuthUtils.generateSalt();
    this.hash = PasswordHashGenerator.computePasswordHash(update.getPassword().toCharArray(), salt, iterations);
  }

  @JsonProperty
  public byte[] getSalt()
  {
    return salt;
  }

  @JsonProperty
  public byte[] getHash()
  {
    return hash;
  }

  @JsonProperty
  public int getIterations()
  {
    return iterations;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || !getClass().equals(o.getClass())) {
      return false;
    }

    BasicAuthenticatorCredentials that = (BasicAuthenticatorCredentials) o;

    if (getIterations() != that.getIterations()) {
      return false;
    }

    return Arrays.equals(getSalt(), that.getSalt()) && Arrays.equals(getHash(), that.getHash());
  }

  @Override
  public int hashCode()
  {
    int result = Arrays.hashCode(getSalt());
    result = 31 * result + Arrays.hashCode(getHash());
    result = 31 * result + getIterations();
    return result;
  }
}
