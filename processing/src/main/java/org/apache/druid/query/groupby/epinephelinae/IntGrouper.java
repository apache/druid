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

package org.apache.druid.query.groupby.epinephelinae;

import com.google.common.base.Preconditions;

import java.util.function.ToIntFunction;

/**
 * {@link Grouper} specialized for the primitive int type
 */
public interface IntGrouper extends Grouper<Integer>
{
  default AggregateResult aggregate(int key)
  {
    return aggregateKeyHash(hashFunction().apply(key));
  }

  AggregateResult aggregateKeyHash(int keyHash);

  /**
   * {@inheritDoc}
   *
   * @deprecated Please use {@link #aggregate(int)} instead.
   */
  @Deprecated
  @Override
  default AggregateResult aggregate(Integer key)
  {
    Preconditions.checkNotNull(key);
    return aggregate(key.intValue());
  }

  /**
   * {@inheritDoc}
   *
   * @deprecated Please use {@link #aggregateKeyHash(int)} instead.
   */
  @Deprecated
  @Override
  default AggregateResult aggregate(Integer key, int keyHash)
  {
    Preconditions.checkNotNull(key);
    return aggregateKeyHash(keyHash);
  }

  @Override
  IntGrouperHashFunction hashFunction();

  interface IntGrouperHashFunction extends ToIntFunction<Integer>
  {
    @Override
    default int applyAsInt(Integer value)
    {
      return apply(value.intValue());
    }

    int apply(int value);
  }
}
