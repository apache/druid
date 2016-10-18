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

package io.druid.query.metadata.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.java.util.common.StringUtils;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 */
public class ListColumnIncluderator implements ColumnIncluderator
{
  private final Set<String> columns;

  @JsonCreator
  public ListColumnIncluderator(
      @JsonProperty("columns") List<String> columns
  )
  {
    this.columns = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
    this.columns.addAll(columns);
  }

  @JsonProperty
  public Set<String> getColumns()
  {
    return Collections.unmodifiableSet(columns);
  }

  @Override
  public boolean include(String columnName)
  {
    return columns.contains(columnName);
  }

  @Override
  public byte[] getCacheKey()
  {
    int size = 1;
    List<byte[]> columns = Lists.newArrayListWithExpectedSize(this.columns.size());

    for (String column : this.columns) {
      final byte[] bytes = StringUtils.toUtf8(column);
      columns.add(bytes);
      size += bytes.length + 1;
    }

    final ByteBuffer bytes = ByteBuffer.allocate(size).put(LIST_CACHE_PREFIX);
    for (byte[] column : columns) {
      bytes.put(column);
      bytes.put((byte) 0xff);
    }

    return bytes.array();
  }
}
