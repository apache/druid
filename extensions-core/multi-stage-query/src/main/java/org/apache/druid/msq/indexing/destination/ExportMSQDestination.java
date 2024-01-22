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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.storage.StorageConnectorProvider;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

public class ExportMSQDestination implements MSQDestination
{
  public static final String TYPE = "export";
  private final StorageConnectorProvider storageConnectorProvider;
  private final ResultFormat resultFormat;
  @Nullable
  private final List<Interval> replaceTimeChunks;

  @JsonCreator
  public ExportMSQDestination(@JsonProperty("storageConnectorProvider") StorageConnectorProvider storageConnectorProvider,
                              @JsonProperty("resultFormat") ResultFormat resultFormat,
                              @JsonProperty("replaceTimeChunks") @Nullable List<Interval> replaceTimeChunks
  )
  {
    this.storageConnectorProvider = storageConnectorProvider;
    this.resultFormat = resultFormat;
    if (replaceTimeChunks == null || Intervals.ONLY_ETERNITY.equals(replaceTimeChunks)) {
      this.replaceTimeChunks = replaceTimeChunks;
    } else {
      throw DruidException.forPersona(DruidException.Persona.USER)
                          .ofCategory(DruidException.Category.UNSUPPORTED)
                          .build("Currently export only works with ");
    }
  }

  @JsonProperty("storageConnectorProvider")
  public StorageConnectorProvider getStorageConnectorProvider()
  {
    return storageConnectorProvider;
  }

  @JsonProperty("resultFormat")
  public ResultFormat getResultFormat()
  {
    return resultFormat;
  }

  @Nullable
  @JsonProperty("replaceTimeChunks")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<Interval> getReplaceTimeChunks()
  {
    return replaceTimeChunks;
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
    return Objects.equals(storageConnectorProvider, that.storageConnectorProvider)
           && resultFormat == that.resultFormat
           && Objects.equals(replaceTimeChunks, that.replaceTimeChunks);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(storageConnectorProvider, resultFormat, replaceTimeChunks);
  }

  @Override
  public String toString()
  {
    return "ExportMSQDestination{" +
           "storageConnectorProvider=" + storageConnectorProvider +
           ", resultFormat=" + resultFormat +
           ", replaceTimeChunks=" + replaceTimeChunks +
           '}';
  }
}
