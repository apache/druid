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

package org.apache.druid.msq.indexing.destination;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.msq.querykit.ShuffleSpecFactories;
import org.apache.druid.msq.querykit.ShuffleSpecFactory;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.storage.ExportStorageProvider;

import java.util.Objects;
import java.util.Optional;

/**
 * Destination used by tasks that write the results as files to an external destination. {@link #resultFormat} denotes
 * the format of the file created and {@link #exportStorageProvider} denotes the type of external
 * destination.
 */
public class ExportMSQDestination implements MSQDestination
{
  public static final String TYPE = "export";
  private final ExportStorageProvider exportStorageProvider;
  private final ResultFormat resultFormat;

  @JsonCreator
  public ExportMSQDestination(
      @JsonProperty("exportStorageProvider") ExportStorageProvider exportStorageProvider,
      @JsonProperty("resultFormat") ResultFormat resultFormat
  )
  {
    this.exportStorageProvider = exportStorageProvider;
    this.resultFormat = resultFormat;
  }



  @JsonProperty("exportStorageProvider")
  public ExportStorageProvider getExportStorageProvider()
  {
    return exportStorageProvider;
  }

  @JsonProperty("resultFormat")
  public ResultFormat getResultFormat()
  {
    return resultFormat;
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
    ExportMSQDestination that = (ExportMSQDestination) o;
    return Objects.equals(exportStorageProvider, that.exportStorageProvider)
           && resultFormat == that.resultFormat;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(exportStorageProvider, resultFormat);
  }

  @Override
  public String toString()
  {
    return "ExportMSQDestination{" +
           "exportStorageProvider=" + exportStorageProvider +
           ", resultFormat=" + resultFormat +
           '}';
  }

  @Override
  public ShuffleSpecFactory getShuffleSpecFactory(int targetSize)
  {
    return ShuffleSpecFactories.getGlobalSortWithTargetSize(targetSize);
  }

  @Override
  public Optional<Resource> getDestinationResource()
  {
    return Optional.of(new Resource(getExportStorageProvider().getResourceType(), ResourceType.EXTERNAL));
  }
}
