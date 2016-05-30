/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.extraction;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.commons.collections.keyvalue.MultiKey;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

@JsonTypeName("multiKeyMap")
public class MultiKeyMapLookupExtractor extends MapLookupExtractor
{
  private final List<List<String>> keyValueList;

  private final boolean isOneToOne;

  @JsonCreator
  public MultiKeyMapLookupExtractor(
      @JsonProperty("keyValueList") List<List<String>> keyValueList,
      @JsonProperty("isOneToOne") boolean isOneToOne
  )
  {
    super(makeMap(keyValueList), isOneToOne);
    this.keyValueList = keyValueList;
    this.isOneToOne = isOneToOne;
  }

  private static Map<Object, String> makeMap(List<List<String>> keyValueList)
  {
    Map<Object, String> map = Maps.newHashMap();

    for (List<String> keyValue: keyValueList)
    {
      Preconditions.checkArgument(keyValue.size() >= 2);
      int keySize = keyValue.size() - 1;
      String[] multiKey = new String[keySize];
      for (int idx = 0; idx < keySize; idx++)
      {
        multiKey[idx] = keyValue.get(idx);
      }
      map.put(new MultiKey(multiKey, false), keyValue.get(keySize));
    }

    return map;
  }

  @JsonProperty
  public List<List<String>> getKeyValueList()
  {
    return keyValueList;
  }

  @Override
  @JsonProperty("isOneToOne")
  public boolean isOneToOne()
  {
    return isOneToOne;
  }

  @Nullable
  @Override
  public String apply(@NotNull Object key)
  {
    return super.apply(key);
  }

  @Override
  public List<Object> unapply(String value)
  {
    return super.unapply(value);
  }

  @Override
  public byte[] getCacheKey()
  {
    return super.getCacheKey();
  }
}
