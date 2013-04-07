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

package com.metamx.druid.query;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.metamx.common.guava.Sequence;
import com.metamx.druid.Query;

/**
*/
public interface CacheStrategy<T, CacheType, QueryType extends Query<T>>
{
  public byte[] computeCacheKey(QueryType query);

  public TypeReference<CacheType> getCacheObjectClazz();

  public Function<T, CacheType> prepareForCache();

  public Function<CacheType, T> pullFromCache();

  public Sequence<T> mergeSequences(Sequence<Sequence<T>> seqOfSequences);
}
