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

import java.util.Collections;
import java.util.List;

public class SpnegoFilterConfig
{

  public static final List<String> DEFAULT_EXCLUDED_PATHS = Collections.emptyList();

  @JsonProperty
  private final String principal;

  @JsonProperty
  private final String keytab;

  @JsonProperty
  private final String authToLocal;

  @JsonProperty
  private final List<String> excludedPaths;

  @JsonProperty
  private final String cookieSignatureSecret;

  @JsonCreator
  public SpnegoFilterConfig(
      @JsonProperty("principal") String principal,
      @JsonProperty("keytab") String keytab,
      @JsonProperty("authToLocal") String authToLocal,
      @JsonProperty("excludedPaths") List<String> excludedPaths,
      @JsonProperty("cookieSignatureSecret") String cookieSignatureSecret
  )
  {
    this.principal = principal;
    this.keytab = keytab;
    this.authToLocal = authToLocal == null ? "DEFAULT" : authToLocal;
    this.excludedPaths = excludedPaths == null ? DEFAULT_EXCLUDED_PATHS : excludedPaths;
    this.cookieSignatureSecret = cookieSignatureSecret;
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
  public String getAuthToLocal()
  {
    return authToLocal;
  }

  @JsonProperty
  public List<String> getExcludedPaths()
  {
    return excludedPaths;
  }

  @JsonProperty
  public String getCookieSignatureSecret()
  {
    return cookieSignatureSecret;
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

    SpnegoFilterConfig that = (SpnegoFilterConfig) o;

    if (principal != null ? !principal.equals(that.principal) : that.principal != null) {
      return false;
    }
    if (keytab != null ? !keytab.equals(that.keytab) : that.keytab != null) {
      return false;
    }
    if (authToLocal != null ? !authToLocal.equals(that.authToLocal) : that.authToLocal != null) {
      return false;
    }
    if (excludedPaths != null ? !excludedPaths.equals(that.excludedPaths) : that.excludedPaths != null) {
      return false;
    }
    return cookieSignatureSecret != null
           ? cookieSignatureSecret.equals(that.cookieSignatureSecret)
           : that.cookieSignatureSecret == null;

  }

  @Override
  public int hashCode()
  {
    int result = principal != null ? principal.hashCode() : 0;
    result = 31 * result + (keytab != null ? keytab.hashCode() : 0);
    result = 31 * result + (authToLocal != null ? authToLocal.hashCode() : 0);
    result = 31 * result + (excludedPaths != null ? excludedPaths.hashCode() : 0);
    result = 31 * result + (cookieSignatureSecret != null ? cookieSignatureSecret.hashCode() : 0);
    return result;
  }
}
