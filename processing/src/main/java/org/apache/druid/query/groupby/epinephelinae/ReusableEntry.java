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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.Arrays;

/**
 * A {@link Grouper.Entry} implementation that is reusable. {@link Grouper#iterator} allows reuse of Entries, and
 * so this class helps implementations taking advantage of that.
 */
public class ReusableEntry<KeyType> implements Grouper.Entry<KeyType>
{
  private KeyType key;

  @Nullable
  private Object[] values;

  @JsonCreator
  public ReusableEntry(
      @JsonProperty("k") @Nullable final KeyType key,
      @JsonProperty("v") @Nullable final Object[] values
  )
  {
    this.key = key;
    this.values = values;
  }

  /**
   * Create a new instance based on a particular {@link Grouper.KeySerde} and a particular Object values size.
   */
  public static <T> ReusableEntry<T> create(final Grouper.KeySerde<T> keySerde, final int numValues)
  {
    return new ReusableEntry<>(keySerde.createKey(), new Object[numValues]);
  }

  /**
   * Returns the key, which enables modifying the key.
   */
  @Override
  @JsonProperty("k")
  public KeyType getKey()
  {
    return key;
  }

  /**
   * Returns the values array, which enables setting individual values.
   */
  @Override
  @JsonProperty("v")
  public Object[] getValues()
  {
    return values;
  }

  /**
   * Replaces the key completely.
   */
  public void setKey(final KeyType key)
  {
    this.key = key;
  }

  /**
   * Replaces the values array completely.
   */
  public void setValues(final Object[] values)
  {
    this.values = values;
  }

  @Override
  public String toString()
  {
    return "ReusableEntry{" +
           "key=" + key +
           ", values=" + Arrays.toString(values) +
           '}';
  }
}
