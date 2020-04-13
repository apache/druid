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

package org.apache.druid.query.metadata.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.StringUtils;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 */
public class ListColumnIncluderator implements ColumnIncluderator
{
  private static final byte[] LIST_CACHE_PREFIX = new byte[]{0x2};

  private final Set<String> columns;

  @JsonCreator
  public ListColumnIncluderator(
      @JsonProperty("columns") List<String> columns
  )
  {
    this.columns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
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
    final List<byte[]> columns = Lists.newArrayListWithExpectedSize(this.columns.size());

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
