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

package org.apache.druid.storage.local;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.storage.StorageConnector;
import org.apache.druid.storage.StorageConnectorProvider;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

@JsonTypeName(LocalFileStorageConnectorProvider.TYPE_NAME)
public class LocalFileStorageConnectorProvider implements StorageConnectorProvider
{
  public static final String TYPE_NAME = "local";

  @JsonProperty
  final File basePath;

  @JsonCreator
  public LocalFileStorageConnectorProvider(@JsonProperty(value = "basePath", required = true) File basePath)
  {
    this.basePath = basePath;
  }

  @Override
  public StorageConnector get()
  {
    try {
      return new LocalFileStorageConnector(basePath);
    }
    catch (IOException e) {
      throw new IAE(
          e,
          "Unable to create storage connector [%s] for base path [%s]",
          LocalFileStorageConnector.class.getSimpleName(),
          basePath
      );
    }
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
    LocalFileStorageConnectorProvider that = (LocalFileStorageConnectorProvider) o;
    return Objects.equals(basePath, that.basePath);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(basePath);
  }
}
