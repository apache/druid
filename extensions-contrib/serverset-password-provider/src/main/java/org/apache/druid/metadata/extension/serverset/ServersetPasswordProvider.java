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

package org.apache.druid.metadata.extension.serverset;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.PasswordProvider;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A PasswordProvider to retrieve a list of bootstrap servers from a serverset. In theory we only
 * need a read in a few bootstrap servers, but this reads all servers for the cluster for now.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ServersetPasswordProvider implements PasswordProvider
{
  private final String fileLocation;
  private static final Logger log = new Logger(ServersetPasswordProvider.class);

  @JsonCreator
  public ServersetPasswordProvider(
      @JsonProperty("file") String filelocation
  )
  {
    this.fileLocation = Preconditions.checkNotNull(filelocation);
  }

  @JsonProperty("file")
  public String getFileLocation()
  {
    return this.fileLocation;
  }

  @JsonIgnore
  @Override
  public String getPassword()
  {
    try {
      return Files.lines(Paths.get(this.fileLocation)).collect(Collectors.joining(","));
    }
    catch (IOException e) {
      log.warn(e, "Encountered IOException while trying to read serverset file!");
      return "";
    }
  }

  @Override
  public String toString()
  {
    return "ServersetPasswordProvider{" +
        "file='" + this.fileLocation + "\'" +
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

    ServersetPasswordProvider that = (ServersetPasswordProvider) o;
    return this.fileLocation.equals(that.fileLocation);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(fileLocation);
  }
}
