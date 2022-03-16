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

package org.apache.druid.segment;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.apache.druid.java.util.common.IAE;

/**
 * Map some set of dictionary id to a smaller set of dictionaryIds (or... the same if you are wasteful), providing
 * a translation layer between them
 */
public class IdMapping
{
  private final Int2IntOpenHashMap forwardMapping;
  private final int[] reverseMapping;

  private IdMapping(Int2IntOpenHashMap forwardMapping, int[] reverseMapping)
  {
    this.forwardMapping = forwardMapping;
    if (forwardMapping.defaultReturnValue() != -1) {
      throw new IAE("forwardMapping.defaultReturnValue() should be -1");
    }
    this.reverseMapping = reverseMapping;
  }

  /**
   * Get the substitute index, given the original
   */
  public int getForwardedId(int id)
  {
    return forwardMapping.get(id);
  }

  /**
   * Get the original index, given the substitute
   */
  public int getReverseId(int id)
  {
    if (id < 0 || id >= reverseMapping.length) {
      return -1;
    }
    return reverseMapping[id];
  }

  /**
   * Get the number of values stored in {@link #forwardMapping}
   */
  public int getValueCardinality()
  {
    return forwardMapping.size();
  }

  public static class Builder
  {
    /**
     * Create a {@link IdMapping} where the cardinality is known, to use with
     * {@link #addMapping(int)} to simultaneously populate both {@link #forwardMapping} and {@link #reverseMapping}.
     */
    public static Builder ofCardinality(int cardinality)
    {
      final Int2IntOpenHashMap forwardMapping = new Int2IntOpenHashMap(cardinality);
      forwardMapping.defaultReturnValue(-1);

      final int[] reverseMapping = new int[cardinality];
      return new Builder(forwardMapping, reverseMapping, true);
    }

    /**
     * Create an {@link IdMapping} where the cardinality is not known up front. Since cardinality
     * is unknown, {@link #reverseMapping} is initialized to an empty array, and {@link #addForwardMapping(int)} should
     * be used to build the {@link #forwardMapping}, when {@link #build()} is called {@link #reverseMapping} will be
     * computed to produce the completed {@link IdMapping}. This is less efficient than
     * {@link #ofCardinality(int)}, so it should be preferred if the value cardinality is in fact known
     */
    public static Builder ofUnknownCardinality()
    {
      final Int2IntOpenHashMap forwardMapping = new Int2IntOpenHashMap();
      forwardMapping.defaultReturnValue(-1);

      return new Builder(forwardMapping, new int[0], false);
    }

    private final Int2IntOpenHashMap forwardMapping;
    private final int[] reverseMapping;
    private int count = 0;
    private final boolean knownCardinality;

    private Builder(
        Int2IntOpenHashMap forwardMapping,
        int[] reverseMapping,
        boolean knownCardinality
    )
    {
      this.forwardMapping = forwardMapping;
      this.reverseMapping = reverseMapping;
      this.knownCardinality = knownCardinality;
    }

    /**
     * Adds a lookup for the 'forwarded' index to the current highest index value, and a reverse lookup from this
     * substitute index back to the original. This method can only be called when cardinality is known, to ensure that
     * {@link #reverseMapping} is initialized correctly.
     */
    public void addMapping(int forwardId)
    {
      if (!knownCardinality) {
        throw new IAE("Cannot call addMapping on unknown cardinality, use addForwardMapping instead");
      }
      forwardMapping.put(forwardId, count);
      reverseMapping[count++] = forwardId;
    }

    /**
     * Adds a lookup for the 'forwarded' index to the current highest index value. This method can only be used when
     * the cardinality is unknown, otherwise {@link #addMapping(int)} should be used instead since the
     * {@link #reverseMapping} is only computed if {@link #knownCardinality} is false
     */

    public void addForwardMapping(int forwardId)
    {
      if (knownCardinality) {
        throw new IAE("Cannot call addForwardMapping on known cardinality, use addMapping instead");
      }
      forwardMapping.put(forwardId, count++);
    }

    public IdMapping build()
    {
      if (knownCardinality) {
        return new IdMapping(forwardMapping, reverseMapping);
      } else {
        final int[] reverseMapping = new int[forwardMapping.size()];
        for (Int2IntMap.Entry e : forwardMapping.int2IntEntrySet()) {
          reverseMapping[e.getIntValue()] = e.getIntKey();
        }
        return new IdMapping(forwardMapping, reverseMapping);
      }
    }
  }
}
