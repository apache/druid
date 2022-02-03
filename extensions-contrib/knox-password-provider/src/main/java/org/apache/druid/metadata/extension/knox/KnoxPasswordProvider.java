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

package org.apache.druid.metadata.extension.knox;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.PasswordProvider;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * A PasswordProvider to retrieve a password from a Knox key. Unfortunately, Druid does not support
 * using a provider for the metadata username, so the user will still need to hardcode the username
 * and the knox key. To this end, a KnoxPasswordProvider instance optionally takes a Knox username
 * as a String. If a username is provided, it will be checked against the username for the primary
 * key returned by Knox and a warning will be emitted if the two do not match.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class KnoxPasswordProvider implements PasswordProvider
{
  private final String knoxKey;
  private final String hostname;
  private final Runtime runtime;
  private static final Logger log = new Logger(KnoxPasswordProvider.class);

  @JsonCreator
  public KnoxPasswordProvider(
      @JsonProperty("knoxKey") String knoxKey
  )
  {
    this.knoxKey = Preconditions.checkNotNull(knoxKey);
    this.runtime = Runtime.getRuntime();
    try {
      this.hostname = InetAddress.getLocalHost().getHostName();
    }
    catch (UnknownHostException e) {
      throw new ISE(e, "Unable to get local host name!");
    }
  }

  @JsonProperty("knoxKey")
  public String getKnoxKey()
  {
    return this.knoxKey;
  }

  @JsonIgnore
  @Override
  public String getPassword()
  {
    String[] args = {"knox", "get", ""};
    args[2] = this.knoxKey;
    String[] envp = new String[1];
    envp[0] = "KNOX_MACHINE_AUTH=" + this.hostname;
    try {
      Process process = runtime.exec(args, envp);
      String[] primaryKey = new BufferedReader(
          new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8)
      )
          .readLine()
          .split("\\|");
      return primaryKey[1];
    }
    catch (IOException e) {
      log.warn(e, "Encountered IOException while reading Knox key!");
      return "";
    }
  }

  @Override
  public String toString()
  {
    return "KnoxPasswordProvider{" +
           "knoxKey='" + this.knoxKey + "\'" +
           "}";
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

    KnoxPasswordProvider that = (KnoxPasswordProvider) o;
    return this.knoxKey.equals(that.knoxKey);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(knoxKey);
  }
}
