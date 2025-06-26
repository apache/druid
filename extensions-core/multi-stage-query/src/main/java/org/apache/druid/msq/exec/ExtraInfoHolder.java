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

package org.apache.druid.msq.exec;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.msq.kernel.WorkOrder;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Holds extra info that will be included in {@link WorkOrder}.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public abstract class ExtraInfoHolder<ExtraInfoType>
{
  public static final String INFO_KEY = "info";

  @Nullable
  private final ExtraInfoType extra;

  public ExtraInfoHolder(@Nullable final ExtraInfoType extra)
  {
    this.extra = extra;
  }

  @JsonProperty(INFO_KEY)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public ExtraInfoType getExtraInfo()
  {
    return extra;
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
    ExtraInfoHolder<?> that = (ExtraInfoHolder<?>) o;
    return Objects.equals(extra, that.extra);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(extra);
  }

  @Override
  public String toString()
  {
    return "ExtraInfoHolder{" +
           "extra=" + extra +
           '}';
  }
}
