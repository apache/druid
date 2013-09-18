/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.metadata.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

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
      final byte[] bytes = column.getBytes(Charsets.UTF_8);
      columns.add(bytes);
      size += bytes.length;
    }

    final ByteBuffer bytes = ByteBuffer.allocate(size).put(LIST_CACHE_PREFIX);
    for (byte[] column : columns) {
      bytes.put(column);
    }

    return bytes.array();
  }
}
