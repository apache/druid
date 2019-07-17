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

package org.apache.druid.query.lookup.namespace.parsers;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.parsers.Parser;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

public class DelegateParser implements Parser<String, String>
{
  private final Parser<String, Object> delegate;
  private final String key;
  private final String value;

  public DelegateParser(
          Parser<String, Object> delegate,
          @NotNull String key,
          @NotNull String value
  )
  {
    this.delegate = delegate;
    this.key = key;
    this.value = value;
  }

  @Override
  public Map<String, String> parseToMap(String input)
  {
    final Map<String, Object> inner = delegate.parseToMap(input);
    final String k = Preconditions.checkNotNull(
        inner.get(key),
        "Key column [%s] missing data in line [%s]",
        key,
        input
    ).toString(); // Just in case is long
    final Object val = inner.get(value);
    if (val == null) {
      // Skip null or missing values, treat them as if there were no row at all.
      return ImmutableMap.of();
    }
    return ImmutableMap.of(k, val.toString());
  }

  @Override
  public void setFieldNames(Iterable<String> fieldNames)
  {
    delegate.setFieldNames(fieldNames);
  }

  @Override
  public List<String> getFieldNames()
  {
    return delegate.getFieldNames();
  }
}
