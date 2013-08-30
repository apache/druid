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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "none", value= NoneColumnIncluderator.class),
    @JsonSubTypes.Type(name = "all", value= AllColumnIncluderator.class),
    @JsonSubTypes.Type(name = "list", value= ListColumnIncluderator.class)
})
public interface ColumnIncluderator
{
  public static final byte[] NONE_CACHE_PREFIX = new byte[]{0x0};
  public static final byte[] ALL_CACHE_PREFIX = new byte[]{0x1};
  public static final byte[] LIST_CACHE_PREFIX = new byte[]{0x2};

  public boolean include(String columnName);
  public byte[] getCacheKey();
}
