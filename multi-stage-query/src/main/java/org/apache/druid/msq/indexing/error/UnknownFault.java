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

package org.apache.druid.msq.indexing.error;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import javax.annotation.Nullable;
import java.util.Objects;

@JsonTypeName(UnknownFault.CODE)
public class UnknownFault extends BaseMSQFault
{
  public static final String CODE = "UnknownError";

  @Nullable
  private final String message;

  private UnknownFault(@Nullable final String message)
  {
    super(CODE, message);
    this.message = message;
  }

  @JsonCreator
  public static UnknownFault forMessage(@JsonProperty("message") @Nullable final String message)
  {
    return new UnknownFault(message);
  }

  public static UnknownFault forException(@Nullable final Throwable t)
  {
    return new UnknownFault(t == null ? null : t.toString());
  }

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonProperty
  public String getMessage()
  {
    return message;
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
    if (!super.equals(o)) {
      return false;
    }
    UnknownFault that = (UnknownFault) o;
    return Objects.equals(message, that.message);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), message);
  }
}
