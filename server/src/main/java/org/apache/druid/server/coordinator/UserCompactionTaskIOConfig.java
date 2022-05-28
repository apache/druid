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

package org.apache.druid.server.coordinator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.segment.indexing.BatchIOConfig;
import org.apache.druid.segment.indexing.IOConfig;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Spec containing IO configs for Auto Compaction.
 * This class mimics JSON field names for fields supported in auto compaction with
 * the corresponding fields in {@link IOConfig}.
 * This is done for end-user ease of use. Basically, end-user will use the same syntax / JSON structure to set
 * IO configs for Auto Compaction as they would for any other ingestion task.
 * Note that this class simply holds IO configs and pass it to compaction task spec.
 */
public class UserCompactionTaskIOConfig
{
  private final boolean dropExisting;

  @JsonCreator
  public UserCompactionTaskIOConfig(
      @JsonProperty("dropExisting") @Nullable Boolean dropExisting
  )
  {
    this.dropExisting = dropExisting == null ? BatchIOConfig.DEFAULT_DROP_EXISTING : dropExisting;
  }

  @JsonProperty
  public boolean isDropExisting()
  {
    return dropExisting;
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
    UserCompactionTaskIOConfig that = (UserCompactionTaskIOConfig) o;
    return dropExisting == that.dropExisting;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dropExisting);
  }

  @Override
  public String toString()
  {
    return "UserCompactionTaskIOConfig{" +
           "dropExisting=" + dropExisting +
           '}';
  }
}
