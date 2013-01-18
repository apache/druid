/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.client.cache;

import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.Map;

/**
 */
public interface Cache
{
  public byte[] get(NamedKey key);
  public void put(NamedKey key, byte[] value);
  public Map<NamedKey, byte[]> getBulk(Iterable<NamedKey> keys);

  public void close(String namespace);

  public CacheStats getStats();

  public class NamedKey
  {
    final public String namespace;
    final public byte[] key;

    public NamedKey(String namespace, byte[] key) {
      Preconditions.checkArgument(namespace != null, "namespace must not be null");
      Preconditions.checkArgument(key != null, "key must not be null");
      this.namespace = namespace;
      this.key = key;
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

      NamedKey namedKey = (NamedKey) o;

      if (!namespace.equals(namedKey.namespace)) {
        return false;
      }
      if (!Arrays.equals(key, namedKey.key)) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode()
    {
      int result = namespace.hashCode();
      result = 31 * result + Arrays.hashCode(key);
      return result;
    }
  }
}
