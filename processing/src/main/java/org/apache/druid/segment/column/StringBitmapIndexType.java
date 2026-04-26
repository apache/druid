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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Objects;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = StringBitmapIndexType.DictionaryEncodedValueIndex.class, name = StringBitmapIndexType.TYPE_DICTIONARY),
    @JsonSubTypes.Type(value = StringBitmapIndexType.NoIndex.class, name = StringBitmapIndexType.TYPE_NONE)
})
public abstract class StringBitmapIndexType
{
  protected static final String TYPE_DICTIONARY = "dictionaryEncodedValueIndex";
  protected static final String TYPE_NONE = "none";

  public abstract boolean hasBitmapIndex();

  public static class DictionaryEncodedValueIndex extends StringBitmapIndexType
  {
    public static final DictionaryEncodedValueIndex INSTANCE = new DictionaryEncodedValueIndex();

    @Override
    public boolean hasBitmapIndex()
    {
      return true;
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
      return Objects.hashCode(getClass());
    }

    @Override
    public String toString()
    {
      return TYPE_DICTIONARY;
    }
  }

  public static class NoIndex extends StringBitmapIndexType
  {
    public static final NoIndex INSTANCE = new NoIndex();

    @Override
    public boolean hasBitmapIndex()
    {
      return false;
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
      return Objects.hashCode(getClass());
    }

    @Override
    public String toString()
    {
      return TYPE_NONE;
    }
  }
}
