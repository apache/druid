/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.client.cache;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import com.metamx.common.StringUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
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

  public boolean isLocal();

  public Collection<String> getNamespaces();

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

    public byte[] toByteArray() {
      final byte[] nsBytes = StringUtils.toUtf8(this.namespace);
      return ByteBuffer.allocate(Ints.BYTES + nsBytes.length + this.key.length)
          .putInt(nsBytes.length)
          .put(nsBytes)
          .put(this.key).array();
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
