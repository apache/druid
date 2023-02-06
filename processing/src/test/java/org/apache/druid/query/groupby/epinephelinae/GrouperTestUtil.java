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

import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

public class GrouperTestUtil
{
  private GrouperTestUtil()
  {
    // No instantiation
  }

  public static Grouper.KeySerde<IntKey> intKeySerde()
  {
    return IntKeySerde.INSTANCE;
  }

  public static TestColumnSelectorFactory newColumnSelectorFactory()
  {
    return new TestColumnSelectorFactory();
  }

  public static <T> List<Grouper.Entry<T>> sortedEntries(
      final Iterator<Grouper.Entry<T>> entryIterator,
      final Function<T, T> keyCopyFn,
      final Comparator<T> keyComparator
  )
  {
    final List<Grouper.Entry<T>> retVal = new ArrayList<>();

    while (entryIterator.hasNext()) {
      final Grouper.Entry<T> entry = entryIterator.next();
      final Object[] valuesCopy = new Object[entry.getValues().length];
      System.arraycopy(entry.getValues(), 0, valuesCopy, 0, entry.getValues().length);

      final ReusableEntry<T> entryCopy = new ReusableEntry<>(keyCopyFn.apply(entry.getKey()), valuesCopy);
      retVal.add(entryCopy);
    }

    retVal.sort(Comparator.comparing(Grouper.Entry::getKey, keyComparator));
    return retVal;
  }

  public static <T> void assertEntriesEquals(
      final Iterator<Grouper.Entry<T>> expectedEntries,
      final Iterator<Grouper.Entry<T>> actualEntries
  )
  {
    int i = 0;
    while (expectedEntries.hasNext() && actualEntries.hasNext()) {
      assertEntriesEqual(StringUtils.format("entry [%d]", i), expectedEntries.next(), actualEntries.next());
      i++;
    }

    if (expectedEntries.hasNext()) {
      Assert.fail(StringUtils.format("expected additional entry [%,d]", i));
    }

    if (actualEntries.hasNext()) {
      Assert.fail(StringUtils.format("encountered too many entries [%,d]", i));
    }
  }

  public static <T> void assertEntriesEqual(
      final Grouper.Entry<T> expectedEntry,
      final Grouper.Entry<T> actualEntry
  )
  {
    assertEntriesEqual(null, expectedEntry, actualEntry);
  }

  public static <T> void assertEntriesEqual(
      @Nullable final String message,
      final Grouper.Entry<T> expectedEntry,
      final Grouper.Entry<T> actualEntry
  )
  {
    Assert.assertEquals(StringUtils.format("%s: key", message), expectedEntry.getKey(), actualEntry.getKey());

    Assert.assertArrayEquals(
        StringUtils.format("%s: values", message),
        expectedEntry.getValues(),
        actualEntry.getValues()
    );
  }
}
