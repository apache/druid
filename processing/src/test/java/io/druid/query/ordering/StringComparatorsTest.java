/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.query.ordering;

import com.google.common.collect.Ordering;
import com.google.common.primitives.UnsignedBytes;
import com.metamx.common.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.text.Collator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Random;

public class StringComparatorsTest
{
  // See
  // https://docs.oracle.com/javase/tutorial/i18n/text/locale.html

  public final static List<String> STRINGS = new ArrayList<>(Arrays.asList(
      "peach",
      "péché",
      "pêche",
      "sin",
      "",
      "☃",
      "C",
      "c",
      "Ç",
      "ç",
      "G",
      "g",
      "Ğ",
      "ğ",
      "I",
      "ı",
      "İ",
      "i",
      "O",
      "o",
      "Ö",
      "ö",
      "S",
      "s",
      "Ş",
      "ş",
      "U",
      "u",
      "Ü",
      "ü",
      "ä",
      "\uD841\uDF0E",
      "\uD841\uDF31",
      "\uD844\uDC5C",
      "\uD84F\uDCB7",
      "\uD860\uDEE2",
      "\uD867\uDD98",
      "\u006E\u0303",
      "\u006E",
      "\uFB00",
      "\u0066\u0066",
      "Å",
      "\u00C5",
      "\u212B"
  ));

  @BeforeClass
  public static void setUp()
  {
    final Random random = new Random(67531513679L);
    final List<String> more = new ArrayList<>(100);
    for (int i = 0; i < 100; ++i) {
      int n = random.nextInt(5);
      final StringBuilder sb = new StringBuilder();
      for (int j = 0; j < n; ++j) {
        sb.append(STRINGS.get(random.nextInt(STRINGS.size())));
      }
      more.add(sb.toString());
    }
    STRINGS.addAll(more);
    Collections.shuffle(STRINGS, random);
    Collections.sort(
        STRINGS,
        new Comparator<String>()
        {
          @Override
          public int compare(String o1, String o2)
          {
            return UnsignedBytes.lexicographicalComparator().compare(
                StringUtils.toUtf8(o1),
                StringUtils.toUtf8(o2)
            );
          }
        }
    );
  }

  @Test
  public void testNaturalSorting()
  {
    final List<String> other = new ArrayList<>(STRINGS);
    Collections.sort(other, Ordering.natural());
    Assert.assertEquals(STRINGS, other);
  }

  @Test
  public void testLexicographicSorting()
  {
    final List<String> other = new ArrayList<>(STRINGS);
    Collections.sort(other, StringComparators.LEXICOGRAPHIC);
    Assert.assertEquals(STRINGS, other);
  }

  @Test
  public void testCollatorSorting()
  {
    final List<String> other = new ArrayList<>(STRINGS);
    final Collator collator = Collator.getInstance(Locale.US);
    Collections.sort(other, new Comparator<String>()
    {
      @Override
      public int compare(String o1, String o2)
      {
        return collator.compare(o1, o2);
      }
    });
    Assert.assertEquals(STRINGS, other);
  }
}
