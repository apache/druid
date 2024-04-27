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

package org.apache.druid.query.aggregation.datasketches.hll;

import com.google.common.collect.ImmutableMap;
import org.apache.datasketches.hll.HllSketch;
import org.apache.druid.java.util.common.StringEncoding;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

/**
 * Tests for {@link HllSketchBuildUtil#updateSketch}.
 */
public class HllSketchBuildUtilTest extends InitializedNullHandlingTest
{
  private static final Map<Integer, String> DICTIONARY = ImmutableMap.of(
      1, "bar",
      2, "foo"
  );

  private final HllSketch sketch = new HllSketch(HllSketch.DEFAULT_LG_K);

  @Test
  public void testUpdateSketchListsOfStringsUTF16LE()
  {
    updateSketch(
        StringEncoding.UTF16LE,
        Arrays.asList("1", "2"),
        Arrays.asList("2", "", "3", "11"),
        Arrays.asList("1", null, "3", "12"),
        Arrays.asList("1", "3", "13")
    );

    assertSketchEstimate(6);
  }

  @Test
  public void testUpdateSketchListsOfStringsUTF8()
  {
    updateSketch(
        StringEncoding.UTF16LE,
        Arrays.asList("1", "2"),
        Arrays.asList("2", "", "3", "11"),
        Arrays.asList("1", null, "3", "12"),
        Arrays.asList("1", "3", "13")
    );

    assertSketchEstimate(6);
  }

  @Test
  public void testUpdateSketchCharArray()
  {
    updateSketch(
        StringEncoding.UTF16LE,
        new char[]{1, 2},
        new char[]{2, 3, 11},
        new char[]{1, 2},
        new char[]{1, 3, 13}
    );

    assertSketchEstimate(3);
  }

  @Test
  public void testUpdateSketchByteArray()
  {
    updateSketch(
        StringEncoding.UTF16LE,
        new byte[]{1, 2},
        new byte[]{2, 3, 11},
        new byte[]{1, 2},
        new byte[]{1, 3, 13}
    );

    assertSketchEstimate(3);
  }

  @Test
  public void testUpdateSketchIntArray()
  {
    updateSketch(
        StringEncoding.UTF16LE,
        new int[]{1, 2},
        new int[]{2, 3, 11},
        new int[]{1, 2},
        new int[]{1, 3, 13}
    );

    assertSketchEstimate(3);
  }

  @Test
  public void testUpdateSketchLongArray()
  {
    updateSketch(
        StringEncoding.UTF16LE,
        new long[]{1, 2},
        new long[]{2, 3, 11},
        new long[]{1, 2},
        new long[]{1, 3, 13}
    );

    assertSketchEstimate(3);
  }

  @Test
  public void testUpdateSketchWithDictionarySelector8to8()
  {
    final TestDictionarySelector selector = new TestDictionarySelector(DICTIONARY, true);
    updateSketchWithDictionarySelector(StringEncoding.UTF8, selector, 0, 1, 2, 1);
    assertSketchEstimate(2);
  }

  @Test
  public void testUpdateSketchWithDictionarySelector8to16()
  {
    final TestDictionarySelector selector = new TestDictionarySelector(DICTIONARY, true);
    updateSketchWithDictionarySelector(StringEncoding.UTF16LE, selector, 0, 1, 2, 1);
    assertSketchEstimate(2);
  }

  @Test
  public void testUpdateSketchWithDictionarySelector16to8()
  {
    final TestDictionarySelector selector = new TestDictionarySelector(DICTIONARY, false);
    updateSketchWithDictionarySelector(StringEncoding.UTF8, selector, 0, 1, 2, 1);
    assertSketchEstimate(2);
  }

  @Test
  public void testUpdateSketchWithDictionarySelector16to16()
  {
    final TestDictionarySelector selector = new TestDictionarySelector(DICTIONARY, false);
    updateSketchWithDictionarySelector(StringEncoding.UTF16LE, selector, 0, 1, 2, 1);
    assertSketchEstimate(2);
  }

  @Test
  public void testUpdateSketchWithDictionarySelectorMixedTo8()
  {
    final TestDictionarySelector selector1 = new TestDictionarySelector(DICTIONARY, false);
    final TestDictionarySelector selector2 = new TestDictionarySelector(DICTIONARY, true);
    updateSketchWithDictionarySelector(StringEncoding.UTF8, selector1, 0, 1, 2, 1);
    updateSketchWithDictionarySelector(StringEncoding.UTF8, selector2, 0, 1, 2, 1);
    assertSketchEstimate(2); // Duplicates are de-duplicated
  }

  @Test
  public void testUpdateSketchWithDictionarySelectorMixedTo16()
  {
    final TestDictionarySelector selector1 = new TestDictionarySelector(DICTIONARY, false);
    final TestDictionarySelector selector2 = new TestDictionarySelector(DICTIONARY, true);
    updateSketchWithDictionarySelector(StringEncoding.UTF16LE, selector1, 0, 1, 2, 1);
    updateSketchWithDictionarySelector(StringEncoding.UTF16LE, selector2, 0, 1, 2, 1);
    assertSketchEstimate(2); // Duplicates are de-duplicated
  }

  @Test
  public void testUpdateSketchWithDictionarySelector8ToMixed()
  {
    final TestDictionarySelector selector = new TestDictionarySelector(DICTIONARY, true);
    updateSketchWithDictionarySelector(StringEncoding.UTF8, selector, 0, 1, 2, 1);
    updateSketchWithDictionarySelector(StringEncoding.UTF16LE, selector, 0, 1, 2, 1);
    assertSketchEstimate(4); // Incompatible hashes
  }

  @Test
  public void testUpdateSketchWithDictionarySelector16ToMixed()
  {
    final TestDictionarySelector selector = new TestDictionarySelector(DICTIONARY, false);
    updateSketchWithDictionarySelector(StringEncoding.UTF8, selector, 0, 1, 2, 1);
    updateSketchWithDictionarySelector(StringEncoding.UTF16LE, selector, 0, 1, 2, 1);
    assertSketchEstimate(4); // Incompatible hashes
  }

  private void updateSketch(final StringEncoding stringEncoding, final Object first, final Object... others)
  {
    // first != null check mimics how updateSketch is called: it's always guarded by a null check on the outer value.
    if (first != null) {
      HllSketchBuildUtil.updateSketch(sketch, stringEncoding, first);
    }

    for (final Object o : others) {
      if (o != null) {
        HllSketchBuildUtil.updateSketch(sketch, stringEncoding, o);
      }
    }
  }

  private void updateSketchWithDictionarySelector(
      final StringEncoding stringEncoding,
      final DimensionDictionarySelector selector,
      final int... ids
  )
  {
    for (int id : ids) {
      HllSketchBuildUtil.updateSketchWithDictionarySelector(sketch, stringEncoding, selector, id);
    }
  }

  private void assertSketchEstimate(final long estimate)
  {
    Assert.assertEquals((double) estimate, sketch.getEstimate(), 0.1);
  }

  private static class TestDictionarySelector implements DimensionDictionarySelector
  {
    private final Map<Integer, String> dictionary;
    private final boolean supportsLookupNameUtf8;

    public TestDictionarySelector(final Map<Integer, String> dictionary, final boolean supportsLookupNameUtf8)
    {
      this.dictionary = dictionary;
      this.supportsLookupNameUtf8 = supportsLookupNameUtf8;
    }

    @Override
    public int getValueCardinality()
    {
      // Unused by this test
      throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public String lookupName(int id)
    {
      return dictionary.get(id);
    }

    @Nullable
    @Override
    public ByteBuffer lookupNameUtf8(int id)
    {
      if (!supportsLookupNameUtf8) {
        throw new UnsupportedOperationException();
      }

      final String s = dictionary.get(id);

      if (s == null) {
        return null;
      } else {
        return ByteBuffer.wrap(StringUtils.toUtf8(s));
      }
    }

    @Override
    public boolean supportsLookupNameUtf8()
    {
      return supportsLookupNameUtf8;
    }

    @Override
    public boolean nameLookupPossibleInAdvance()
    {
      return true;
    }

    @Nullable
    @Override
    public IdLookup idLookup()
    {
      return null;
    }
  }
}
