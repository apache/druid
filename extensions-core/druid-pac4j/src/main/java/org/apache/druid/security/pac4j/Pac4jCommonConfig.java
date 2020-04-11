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

package org.apache.druid.security.pac4j;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.metadata.PasswordProvider;
import org.joda.time.Duration;

public class Pac4jCommonConfig
{
  @JsonProperty
  private final boolean enableCustomSslContext;

  @JsonProperty
  private final PasswordProvider cookiePassphrase;

  @JsonProperty
  private final Duration readTimeout;

  @JsonCreator
  public Pac4jCommonConfig(
      @JsonProperty("enableCustomSslContext") boolean enableCustomSslContext,
      @JsonProperty("cookiePassphrase") PasswordProvider cookiePassphrase,
      @JsonProperty("readTimeout") Duration readTimeout
  )
  {
    this.enableCustomSslContext = enableCustomSslContext;
    this.cookiePassphrase = Preconditions.checkNotNull(cookiePassphrase, "null cookiePassphrase");
    this.readTimeout = readTimeout == null ? Duration.millis(5000) : readTimeout;
  }

  @JsonProperty
  public boolean isEnableCustomSslContext()
  {
    return enableCustomSslContext;
  }

  @JsonProperty
  public PasswordProvider getCookiePassphrase()
  {
    return cookiePassphrase;
  }

  @JsonProperty
  public Duration getReadTimeout()
  {
    return readTimeout;
  }
}
