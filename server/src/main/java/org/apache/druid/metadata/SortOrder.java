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

package org.apache.druid.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.StringUtils;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Specifies the sort order when doing metadata store queries.
 */
public enum SortOrder
{
  ASC("ASC"),

  DESC("DESC");

  private String value;

  SortOrder(String value)
  {
    this.value = value;
  }

  @Override
  @JsonValue
  public String toString()
  {
    return String.valueOf(value);
  }

  @JsonCreator
  public static SortOrder fromValue(String value)
  {
    for (SortOrder b : SortOrder.values()) {
      if (String.valueOf(b.value).equalsIgnoreCase(String.valueOf(value))) {
        return b;
      }
    }
    throw InvalidInput.exception(StringUtils.format(
        "Unexpected value[%s] for SortOrder. Possible values are: %s",
        value, Arrays.stream(SortOrder.values()).map(SortOrder::toString).collect(Collectors.toList())
    ));
  }
}
