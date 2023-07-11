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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.List;
import java.util.Objects;

@JsonTypeName(TooManyRowsWithSameKeyFault.CODE)
public class TooManyRowsWithSameKeyFault extends BaseMSQFault
{
  static final String CODE = "TooManyRowsWithSameKey";

  private final List<Object> key;
  private final long numBytes;
  private final long maxBytes;

  @JsonCreator
  public TooManyRowsWithSameKeyFault(
      @JsonProperty("key") final List<Object> key,
      @JsonProperty("numBytes") final long numBytes,
      @JsonProperty("maxBytes") final long maxBytes
  )
  {
    super(
        CODE,
        "Too many rows with the same key during sort-merge join (bytes buffered = %,d; limit = %,d). Key: %s",
        numBytes,
        maxBytes,
        key
    );

    this.key = key;
    this.numBytes = numBytes;
    this.maxBytes = maxBytes;
  }

  @JsonProperty
  public List<Object> getKey()
  {
    return key;
  }

  @JsonProperty
  public long getNumBytes()
  {
    return numBytes;
  }

  @JsonProperty
  public long getMaxBytes()
  {
    return maxBytes;
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
    TooManyRowsWithSameKeyFault that = (TooManyRowsWithSameKeyFault) o;
    return numBytes == that.numBytes && maxBytes == that.maxBytes && Objects.equals(key, that.key);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), key, numBytes, maxBytes);
  }
}
