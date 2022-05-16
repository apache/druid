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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.segment.indexing.BatchIOConfig;
import org.apache.druid.segment.indexing.IOConfig;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * {@link IOConfig} for {@link CompactionTask}. Should be synchronized with {@link
 * org.apache.druid.client.indexing.ClientCompactionIOConfig}.
 *
 * @see CompactionInputSpec
 */
@JsonTypeName("compact")
public class CompactionIOConfig implements IOConfig
{
  private final CompactionInputSpec inputSpec;
  private final boolean dropExisting;

  @JsonCreator
  public CompactionIOConfig(
      @JsonProperty("inputSpec") CompactionInputSpec inputSpec,
      @JsonProperty("dropExisting") @Nullable Boolean dropExisting
  )
  {
    this.inputSpec = inputSpec;
    this.dropExisting = dropExisting == null ? BatchIOConfig.DEFAULT_DROP_EXISTING : dropExisting;
  }

  @JsonProperty
  public CompactionInputSpec getInputSpec()
  {
    return inputSpec;
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
    CompactionIOConfig that = (CompactionIOConfig) o;
    return dropExisting == that.dropExisting &&
           Objects.equals(inputSpec, that.inputSpec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(inputSpec, dropExisting);
  }

  @Override
  public String toString()
  {
    return "CompactionIOConfig{" +
           "inputSpec=" + inputSpec +
           ", dropExisting=" + dropExisting +
           '}';
  }
}
