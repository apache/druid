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

package org.apache.druid.msq.counters;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.ordering.StringComparators;

import java.util.Comparator;
import java.util.Map;

/**
 * Standard names for counters.
 */
public class CounterNames
{
  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String SHUFFLE = "shuffle";
  private static final String SORT_PROGRESS = "sortProgress";
  private static final String WARNINGS = "warnings";
  private static final Comparator<String> COMPARATOR = new NameComparator();

  private CounterNames()
  {
    // No construction: statics only.
  }

  /**
   * Standard name for an input channel counter created by {@link CounterTracker#channel}.
   */
  public static String inputChannel(final int inputNumber)
  {
    return StringUtils.format("%s%d", INPUT, inputNumber);
  }

  /**
   * Standard name for an output channel counter created by {@link CounterTracker#channel}.
   */
  public static String outputChannel()
  {
    return OUTPUT;
  }

  /**
   * Standard name for a shuffle channel counter created by {@link CounterTracker#channel}.
   */
  public static String shuffleChannel()
  {
    return SHUFFLE;
  }

  /**
   * Standard name for a sort progress counter created by {@link CounterTracker#sortProgress()}.
   */
  public static String sortProgress()
  {
    return SORT_PROGRESS;
  }

  /**
   * Standard name for a warnings counter created by {@link CounterTracker#warnings()}.
   */
  public static String warnings()
  {
    return WARNINGS;
  }

  /**
   * Standard comparator for counter names. Not necessary for functionality, but helps with human-readability.
   */
  public static Comparator<String> comparator()
  {
    return COMPARATOR;
  }

  /**
   * Comparator that ensures counters are sorted in a nice order when serialized to JSON. Not necessary for
   * functionality, but helps with human-readability.
   */
  private static class NameComparator implements Comparator<String>
  {
    private static final Map<String, Integer> ORDER =
        ImmutableMap.<String, Integer>builder()
                    .put(OUTPUT, 0)
                    .put(SHUFFLE, 1)
                    .put(SORT_PROGRESS, 2)
                    .put(WARNINGS, 3)
                    .build();

    @Override
    public int compare(final String name1, final String name2)
    {
      final boolean isInput1 = name1.startsWith(INPUT);
      final boolean isInput2 = name2.startsWith(INPUT);

      if (isInput1 && isInput2) {
        // Compare INPUT alphanumerically, so e.g. "input2" is before "input10"
        return StringComparators.ALPHANUMERIC.compare(name1, name2);
      } else if (isInput1 != isInput2) {
        // INPUT goes first
        return isInput1 ? -1 : 1;
      }

      assert !isInput1 && !isInput2;

      final Integer order1 = ORDER.get(name1);
      final Integer order2 = ORDER.get(name2);

      if (order1 != null && order2 != null) {
        // Respect ordering from ORDER
        return Integer.compare(order1, order2);
      } else if (order1 != null) {
        // Names from ORDER go before names that are not in ORDER
        return -1;
      } else if (order2 != null) {
        // Names from ORDER go before names that are not in ORDER
        return 1;
      } else {
        assert order1 == null && order2 == null;
        return name1.compareTo(name2);
      }
    }
  }
}
