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

package io.druid.client.cache;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import org.apache.commons.codec.digest.DigestUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class CacheImplUtils
{
  static String computeKeyHash(String prefix, String namespace, byte[] key) {
    return String.format("%s:%s:%s", prefix, DigestUtils.sha1Hex(namespace), DigestUtils.sha1Hex(key));
  }

  public static String computeKeyHash(String prefix, Cache.NamedKey key)
  {
    // hash keys to keep things under 250 characters for memcached
    return computeKeyHash(prefix, key.namespace, key.key);
  }

  public static String computeNamespaceHash(String prefix, String namespace) {
    return prefix + ':' + DigestUtils.sha1Hex(namespace);
  }

  public static byte[] deserializeValue(Cache.NamedKey key, byte[] bytes)
  {
    ByteBuffer buf = ByteBuffer.wrap(bytes);

    final int keyLength = buf.getInt();
    byte[] keyBytes = new byte[keyLength];
    buf.get(keyBytes);
    byte[] value = new byte[buf.remaining()];
    buf.get(value);

    Preconditions.checkState(
        Arrays.equals(keyBytes, key.toByteArray()),
        "Keys do not match, possible hash collision?"
    );
    return value;
  }

  public static byte[] serializeValue(Cache.NamedKey key, byte[] value)
  {
    byte[] keyBytes = key.toByteArray();
    return ByteBuffer.allocate(Ints.BYTES + keyBytes.length + value.length)
                     .putInt(keyBytes.length)
                     .put(keyBytes)
                     .put(value)
                     .array();
  }
}
