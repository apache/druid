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

import com.metamx.common.Pair;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 */
public interface CacheBroker
{
  public byte[] get(String identifier, byte[] key);
  public void put(String identifier, byte[] key, byte[] value);
  public Map<Pair<String, ByteBuffer>, byte[]> getBulk(Iterable<Pair<String, ByteBuffer>> identifierKeyPairs);

  public void close(String identifier);

  public CacheStats getStats();
}
