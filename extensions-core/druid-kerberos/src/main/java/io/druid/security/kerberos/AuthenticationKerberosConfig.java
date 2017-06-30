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

package io.druid.security.kerberos;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class AuthenticationKerberosConfig
{
  @JsonProperty
  private final String principal;

  @JsonProperty
  private final String keytab;

  @JsonProperty
  private final String namespace;

  @JsonCreator
  public AuthenticationKerberosConfig(
      @JsonProperty("principal") String principal,
      @JsonProperty("keytab") String keytab,
      @JsonProperty("namespace") String namespace
  )
  {
    this.principal = principal;
    this.keytab = keytab;
    this.namespace = namespace;
  }

  @JsonProperty
  public String getPrincipal()
  {
    return principal;
  }

  @JsonProperty
  public String getKeytab()
  {
    return keytab;
  }

  @JsonProperty
  public String getNamespace()
  {
    return namespace;
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

    AuthenticationKerberosConfig that = (AuthenticationKerberosConfig) o;

    if (getPrincipal() != null ? !getPrincipal().equals(that.getPrincipal()) : that.getPrincipal() != null) {
      return false;
    }
    if (getKeytab() != null ? !getKeytab().equals(that.getKeytab()) : that.getKeytab() != null) {
      return false;
    }
    return getNamespace() != null ? getNamespace().equals(that.getNamespace()) : that.getNamespace() == null;

  }

  @Override
  public int hashCode()
  {
    int result = getPrincipal() != null ? getPrincipal().hashCode() : 0;
    result = 31 * result + (getKeytab() != null ? getKeytab().hashCode() : 0);
    result = 31 * result + (getNamespace() != null ? getNamespace().hashCode() : 0);
    return result;
  }
}
