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

import com.google.common.collect.Iterables;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.segment.data.DictionaryDummyWrapStrategy;
import org.apache.druid.segment.data.DictionarySparseIndexWrapStrategy;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.SparseArrayIndexed;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public class SparseArrayIndexedTest
{
  int cardinality = 64 << 10;
  int searchNum = 10000;
  int strLen = 16;
  int indexGranularity = 1024;
  Comparator<String> comparator = GenericIndexed.STRING_STRATEGY;
  GenericIndexed<String> genericIndexed;
  Indexed<String> dictionary;
  String[] strings;
  Set<String> strSet;
  List<String> elementsToSearch;

  @Before
  public void setup()
  {
    NullHandling.initializeForTests();

    strSet = getStringSet(cardinality, strLen);
    strings = Iterables.toArray(strSet, String.class);
    Arrays.sort(strings, comparator);

    double prob = 1.0D * searchNum / cardinality;
    elementsToSearch = new ArrayList<>(searchNum);
    ThreadLocalRandom random = ThreadLocalRandom.current();
    for (String str : strings) {
      if (random.nextDouble() < prob) {
        elementsToSearch.add(str);
      }
    }
    genericIndexed = GenericIndexed.fromArray(strings, GenericIndexed.STRING_STRATEGY);
    this.dictionary = new SparseArrayIndexed<>(genericIndexed, indexGranularity);
  }

  @Test
  public void testGet()
  {
    Assert.assertEquals(strings.length, dictionary.size());
    Assert.assertEquals(GenericIndexed.STRING_STRATEGY, genericIndexed.getStrategy());
    for (int i = 0; i < dictionary.size(); i++) {
      Assert.assertEquals(
          String.format(
              Locale.getDefault(),
              "index:%d not equal, expect:%s, actual:%s",
              i,
              strings[i],
              dictionary.get(i)
          ),
          strings[i],
          dictionary.get(i)
      );
    }
  }

  @Test
  public void testIndexOf()
  {
    for (String str : elementsToSearch) {
      int expect = Arrays.binarySearch(strings, str, comparator);
      int actual = dictionary.indexOf(str);
      Assert.assertTrue(
          String.format(Locale.getDefault(), "index should be positive, str:%s, index:%d", str, expect),
          expect >= 0
      );
      Assert.assertEquals(
          String.format(Locale.getDefault(), "exepect index:%d not equals actual index:%d", expect, actual),
          expect,
          actual
      );
    }
  }

  @Test
  public void testStrategy()
  {
    DictionaryDummyWrapStrategy dummyWrapStrategy = new DictionaryDummyWrapStrategy();
    Assert.assertEquals(genericIndexed, dummyWrapStrategy.wrap(genericIndexed));

    DictionarySparseIndexWrapStrategy sparseIndexWrapStrategy = new DictionarySparseIndexWrapStrategy(
        256,
        8 << 10,
        512
    );
    Assert.assertTrue(sparseIndexWrapStrategy.wrap(genericIndexed) instanceof SparseArrayIndexed);
  }

  private static Set<String> getStringSet(int n, int size)
  {
    Set<String> strings = new HashSet<>();
    ThreadLocalRandom random = ThreadLocalRandom.current();

    while (strings.size() < n) {
      strings.add(NullHandling.emptyToNullIfNeeded(getString(random.nextInt(size))));
    }
    return strings;
  }

  @Nullable
  private static String getString(int len)
  {
    Random random = ThreadLocalRandom.current();
    if (len == 0 && random.nextBoolean()) {
      return null;
    }
    StringBuilder builder = new StringBuilder(len);
    for (int i = 0; i < len; i++) {
      builder.append(Character.toChars(random.nextInt(Short.MAX_VALUE)));
    }
    return builder.toString();
  }
}
