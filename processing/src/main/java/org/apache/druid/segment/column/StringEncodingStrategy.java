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

package org.apache.druid.segment.column;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
import java.util.Objects;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = StringEncodingStrategy.Utf8.class, name = StringEncodingStrategy.UTF8),
    @JsonSubTypes.Type(value = StringEncodingStrategy.FrontCoded.class, name = StringEncodingStrategy.FRONT_CODED)
})
public interface StringEncodingStrategy
{
  Utf8 DEFAULT = new Utf8();
  String UTF8 = "utf8";
  String FRONT_CODED = "frontCoded";

  byte UTF8_ID = 0x00;
  byte FRONT_CODED_ID = 0x01;

  String getType();

  byte getId();

  class Utf8 implements StringEncodingStrategy
  {
    @Override
    public String getType()
    {
      return UTF8;
    }

    @Override
    public byte getId()
    {
      return UTF8_ID;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      return o != null && getClass() == o.getClass();
    }

    @Override
    public int hashCode()
    {
      return Objects.hashCode(UTF8);
    }

    @Override
    public String toString()
    {
      return "Utf8{}";
    }
  }

  class FrontCoded implements StringEncodingStrategy
  {
    public static final int DEFAULT_BUCKET_SIZE = 4;

    @JsonProperty
    public int getBucketSize()
    {
      return bucketSize;
    }

    @JsonProperty
    private final int bucketSize;

    @JsonCreator
    public FrontCoded(
        @JsonProperty("bucketSize") @Nullable Integer bucketSize
    )
    {
      this.bucketSize = bucketSize == null ? DEFAULT_BUCKET_SIZE : bucketSize;
      if (Integer.bitCount(this.bucketSize) != 1) {
        throw new ISE("bucketSize must be a power of two but was[%,d]", bucketSize);
      }
    }

    @Override
    public String getType()
    {
      return FRONT_CODED;
    }

    @Override
    public byte getId()
    {
      return FRONT_CODED_ID;
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
      FrontCoded that = (FrontCoded) o;
      return bucketSize == that.bucketSize;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(bucketSize);
    }

    @Override
    public String toString()
    {
      return "FrontCoded{" +
             "bucketSize=" + bucketSize +
             '}';
    }
  }
}
