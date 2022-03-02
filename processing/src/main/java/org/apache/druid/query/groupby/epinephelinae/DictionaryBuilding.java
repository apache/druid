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

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.util.ArrayList;
import java.util.List;

/**
 * Utilities for parts of the groupBy engine that need to build dictionaries.
 */
public class DictionaryBuilding
{
  public static final int UNKNOWN_DICTIONARY_ID = -1;

  // Entry in dictionary, node pointer in reverseDictionary, hash + k/v/next pointer in reverseDictionary nodes
  private static final int ROUGH_OVERHEAD_PER_DICTIONARY_ENTRY = Long.BYTES * 5 + Integer.BYTES;

  public static <T> List<T> createDictionary()
  {
    return new ArrayList<>();
  }

  public static <T> Object2IntMap<T> createReverseDictionary()
  {
    final Object2IntOpenHashMap<T> m = new Object2IntOpenHashMap<>();
    m.defaultReturnValue(UNKNOWN_DICTIONARY_ID);
    return m;
  }

  /**
   * Estimated footprint of a new entry.
   */
  public static int estimateEntryFootprint(final int valueFootprint)
  {
    return valueFootprint + ROUGH_OVERHEAD_PER_DICTIONARY_ENTRY;
  }
}
