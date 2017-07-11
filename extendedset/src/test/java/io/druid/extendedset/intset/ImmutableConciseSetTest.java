/*
* Copyright 2012 Metamarkets Group Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package io.druid.extendedset.intset;

import com.google.common.collect.Lists;
import io.druid.java.util.common.StringUtils;
import junit.framework.Assert;
import org.junit.Test;

import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;

/**
 */
public class ImmutableConciseSetTest
{
  public static final int NO_COMPLEMENT_LENGTH = -1;

  @Test
  public void testWordIteratorNext1()
  {
    final int[] ints = {1, 2, 3, 4, 5};
    ConciseSet set = new ConciseSet();
    for (int i : ints) {
      set.add(i);
    }
    ImmutableConciseSet iSet = ImmutableConciseSet.newImmutableFromMutable(set);

    ImmutableConciseSet.WordIterator itr = iSet.newWordIterator();
    Assert.assertEquals(0x8000003E, itr.next());

    Assert.assertEquals(itr.hasNext(), false);
  }

  @Test
  public void testWordIteratorNext2()
  {
    ConciseSet set = new ConciseSet();
    for (int i = 0; i < 100000; i++) {
      set.add(i);

    }
    ImmutableConciseSet iSet = ImmutableConciseSet.newImmutableFromMutable(set);

    ImmutableConciseSet.WordIterator itr = iSet.newWordIterator();
    Assert.assertEquals(0x40000C98, itr.next());
    Assert.assertEquals(0x81FFFFFF, itr.next());
    Assert.assertEquals(itr.hasNext(), false);
  }

  /**
   * Advance to middle of a fill
   */
  @Test
  public void testWordIteratorAdvanceTo1()
  {
    ConciseSet set = new ConciseSet();
    for (int i = 0; i < 100000; i++) {
      set.add(i);

    }
    ImmutableConciseSet iSet = ImmutableConciseSet.newImmutableFromMutable(set);

    ImmutableConciseSet.WordIterator itr = iSet.newWordIterator();
    itr.advanceTo(50);
    Assert.assertEquals(1073744998, itr.next());
    Assert.assertEquals(0x81FFFFFF, itr.next());
    Assert.assertEquals(itr.hasNext(), false);
  }

  /**
   * Advance past a fill directly to a new literal
   */
  @Test
  public void testWordIteratorAdvanceTo2()
  {
    ConciseSet set = new ConciseSet();
    for (int i = 0; i < 100000; i++) {
      set.add(i);

    }
    ImmutableConciseSet iSet = ImmutableConciseSet.newImmutableFromMutable(set);

    ImmutableConciseSet.WordIterator itr = iSet.newWordIterator();
    itr.advanceTo(3225);
    Assert.assertEquals(0x81FFFFFF, itr.next());
    Assert.assertEquals(itr.hasNext(), false);
  }

  @Test
  public void testCompactOneLitOneLit()
  {
    int[] words = {-1, -1};

    ImmutableConciseSet res = ImmutableConciseSet.compact(new ImmutableConciseSet(IntBuffer.wrap(words)));

    ImmutableConciseSet.WordIterator itr = res.newWordIterator();

    Assert.assertEquals(0x40000001, itr.next());
    Assert.assertEquals(itr.hasNext(), false);
  }

  @Test
  public void testCompactOneLitPureOneFill()
  {
    int[] words = {-1, 0x40000004};

    ImmutableConciseSet res = ImmutableConciseSet.compact(new ImmutableConciseSet(IntBuffer.wrap(words)));
    ImmutableConciseSet.WordIterator itr = res.newWordIterator();

    Assert.assertEquals(0x40000005, itr.next());
    Assert.assertEquals(itr.hasNext(), false);
  }

  @Test
  public void testCompactOneLitDirtyOneFill()
  {
    int[] words = {-1, 0x42000004};

    ImmutableConciseSet res = ImmutableConciseSet.compact(new ImmutableConciseSet(IntBuffer.wrap(words)));
    ImmutableConciseSet.WordIterator itr = res.newWordIterator();

    Assert.assertEquals(-1, itr.next());
    Assert.assertEquals(0x42000004, itr.next());
    Assert.assertEquals(itr.hasNext(), false);
  }

  @Test
  public void testCompactOneFillOneLit()
  {
    int[] words = {0x40000004, -1};

    ImmutableConciseSet res = ImmutableConciseSet.compact(new ImmutableConciseSet(IntBuffer.wrap(words)));
    ImmutableConciseSet.WordIterator itr = res.newWordIterator();

    Assert.assertEquals(0x40000005, itr.next());
    Assert.assertEquals(itr.hasNext(), false);
  }

  @Test
  public void testCompactOneFillPureOneFill()
  {
    int[] words = {0x40000004, 0x40000004};

    ImmutableConciseSet res = ImmutableConciseSet.compact(new ImmutableConciseSet(IntBuffer.wrap(words)));
    ImmutableConciseSet.WordIterator itr = res.newWordIterator();

    Assert.assertEquals(0x40000009, itr.next());
    Assert.assertEquals(itr.hasNext(), false);
  }

  @Test
  public void testCompactOneFillDirtyOneFill()
  {
    int[] words = {0x40000004, 0x42000004};

    ImmutableConciseSet res = ImmutableConciseSet.compact(new ImmutableConciseSet(IntBuffer.wrap(words)));
    ImmutableConciseSet.WordIterator itr = res.newWordIterator();

    Assert.assertEquals(0x40000004, itr.next());
    Assert.assertEquals(0x42000004, itr.next());
    Assert.assertEquals(itr.hasNext(), false);
  }

  @Test
  public void testCompactZeroLitZeroLit()
  {
    int[] words = {0x80000000, 0x80000000, -1};

    ImmutableConciseSet res = ImmutableConciseSet.compact(new ImmutableConciseSet(IntBuffer.wrap(words)));
    ImmutableConciseSet.WordIterator itr = res.newWordIterator();

    Assert.assertEquals(0x00000001, itr.next());
    Assert.assertEquals(-1, itr.next());
    Assert.assertEquals(itr.hasNext(), false);
  }

  @Test
  public void testCompactZeroLitPureZeroFill()
  {
    int[] words = {0x80000000, 0x00000004, -1};

    ImmutableConciseSet res = ImmutableConciseSet.compact(new ImmutableConciseSet(IntBuffer.wrap(words)));
    ImmutableConciseSet.WordIterator itr = res.newWordIterator();

    Assert.assertEquals(0x00000005, itr.next());
    Assert.assertEquals(-1, itr.next());
    Assert.assertEquals(itr.hasNext(), false);
  }

  @Test
  public void testCompactZeroLitDirtyZeroFill()
  {
    int[] words = {0x80000000, 0x02000004, -1};

    ImmutableConciseSet res = ImmutableConciseSet.compact(new ImmutableConciseSet(IntBuffer.wrap(words)));
    ImmutableConciseSet.WordIterator itr = res.newWordIterator();

    Assert.assertEquals(0x80000000, itr.next());
    Assert.assertEquals(0x02000004, itr.next());
    Assert.assertEquals(-1, itr.next());
    Assert.assertEquals(itr.hasNext(), false);
  }

  @Test
  public void testCompactZeroFillZeroLit()
  {
    int[] words = {0x00000004, 0x80000000, -1};

    ImmutableConciseSet res = ImmutableConciseSet.compact(new ImmutableConciseSet(IntBuffer.wrap(words)));
    ImmutableConciseSet.WordIterator itr = res.newWordIterator();

    Assert.assertEquals(0x00000005, itr.next());
    Assert.assertEquals(-1, itr.next());
    Assert.assertEquals(itr.hasNext(), false);
  }

  @Test
  public void testCompactZeroFillPureZeroFill()
  {
    int[] words = {0x00000004, 0x00000004, -1};

    ImmutableConciseSet res = ImmutableConciseSet.compact(new ImmutableConciseSet(IntBuffer.wrap(words)));
    ImmutableConciseSet.WordIterator itr = res.newWordIterator();

    Assert.assertEquals(0x00000009, itr.next());
    Assert.assertEquals(-1, itr.next());
    Assert.assertEquals(itr.hasNext(), false);
  }

  @Test
  public void testCompactZeroFillDirtyZeroFill()
  {
    int[] words = {0x00000004, 0x02000004, -1};

    ImmutableConciseSet res = ImmutableConciseSet.compact(new ImmutableConciseSet(IntBuffer.wrap(words)));
    ImmutableConciseSet.WordIterator itr = res.newWordIterator();

    Assert.assertEquals(0x00000004, itr.next());
    Assert.assertEquals(0x02000004, itr.next());
    Assert.assertEquals(-1, itr.next());
    Assert.assertEquals(itr.hasNext(), false);
  }

  @Test
  public void testCompactSingleOneBitLitZeroLit()
  {
    int[] words = {0x80000001, 0x80000000, -1};

    ImmutableConciseSet res = ImmutableConciseSet.compact(new ImmutableConciseSet(IntBuffer.wrap(words)));
    ImmutableConciseSet.WordIterator itr = res.newWordIterator();

    Assert.assertEquals(0x02000001, itr.next());
    Assert.assertEquals(-1, itr.next());
    Assert.assertEquals(itr.hasNext(), false);
  }

  @Test
  public void testCompactDoubleOneBitLitZeroLit()
  {
    int[] words = {0x80000003, 0x80000000, -1};

    ImmutableConciseSet res = ImmutableConciseSet.compact(new ImmutableConciseSet(IntBuffer.wrap(words)));
    ImmutableConciseSet.WordIterator itr = res.newWordIterator();

    Assert.assertEquals(0x80000003, itr.next());
    Assert.assertEquals(0x80000000, itr.next());
    Assert.assertEquals(-1, itr.next());
    Assert.assertEquals(itr.hasNext(), false);
  }

  @Test
  public void testCompactSingleOneBitLitPureZeroFill()
  {
    int[] words = {0x80000001, 0x00000004, -1};

    ImmutableConciseSet res = ImmutableConciseSet.compact(new ImmutableConciseSet(IntBuffer.wrap(words)));
    ImmutableConciseSet.WordIterator itr = res.newWordIterator();

    Assert.assertEquals(0x02000005, itr.next());
    Assert.assertEquals(-1, itr.next());
    Assert.assertEquals(itr.hasNext(), false);
  }

  @Test
  public void testCompactDoubleOneBitLitPureZeroFill()
  {
    int[] words = {0x80000003, 0x00000004, -1};

    ImmutableConciseSet res = ImmutableConciseSet.compact(new ImmutableConciseSet(IntBuffer.wrap(words)));
    ImmutableConciseSet.WordIterator itr = res.newWordIterator();

    Assert.assertEquals(0x80000003, itr.next());
    Assert.assertEquals(0x00000004, itr.next());
    Assert.assertEquals(-1, itr.next());
    Assert.assertEquals(itr.hasNext(), false);
  }

  @Test
  public void testCompactSingleOneBitLitDirtyZeroFill()
  {
    int[] words = {0x80000001, 0x02000004, -1};

    ImmutableConciseSet res = ImmutableConciseSet.compact(new ImmutableConciseSet(IntBuffer.wrap(words)));
    ImmutableConciseSet.WordIterator itr = res.newWordIterator();

    Assert.assertEquals(0x80000001, itr.next());
    Assert.assertEquals(0x02000004, itr.next());
    Assert.assertEquals(-1, itr.next());
    Assert.assertEquals(itr.hasNext(), false);
  }

  @Test
  public void testCompactSingleZeroBitLitOneLit()
  {
    int[] words = {0xFFFFFFFE, -1};

    ImmutableConciseSet res = ImmutableConciseSet.compact(new ImmutableConciseSet(IntBuffer.wrap(words)));
    ImmutableConciseSet.WordIterator itr = res.newWordIterator();

    Assert.assertEquals(0x42000001, itr.next());
    Assert.assertEquals(itr.hasNext(), false);
  }

  @Test
  public void testCompactDoubleZeroBitLitOneLit()
  {
    int[] words = {0xFFFFFFEE, -1};

    ImmutableConciseSet res = ImmutableConciseSet.compact(new ImmutableConciseSet(IntBuffer.wrap(words)));
    ImmutableConciseSet.WordIterator itr = res.newWordIterator();

    Assert.assertEquals(0xFFFFFFEE, itr.next());
    Assert.assertEquals(-1, itr.next());
    Assert.assertEquals(itr.hasNext(), false);
  }

  @Test
  public void testCompactSingleZeroBitLitPureOneFill()
  {
    int[] words = {0xFFFFFFFE, 0x40000004};

    ImmutableConciseSet res = ImmutableConciseSet.compact(new ImmutableConciseSet(IntBuffer.wrap(words)));
    ImmutableConciseSet.WordIterator itr = res.newWordIterator();

    Assert.assertEquals(0x42000005, itr.next());
    Assert.assertEquals(itr.hasNext(), false);
  }

  @Test
  public void testCompactDoubleZeroBitLitPureOneFill()
  {
    int[] words = {0xFFFFFFFC, 0x40000004};

    ImmutableConciseSet res = ImmutableConciseSet.compact(new ImmutableConciseSet(IntBuffer.wrap(words)));
    ImmutableConciseSet.WordIterator itr = res.newWordIterator();

    Assert.assertEquals(0xFFFFFFFC, itr.next());
    Assert.assertEquals(0x40000004, itr.next());
    Assert.assertEquals(itr.hasNext(), false);
  }

  @Test
  public void testCompactSingleZeroBitLitDirtyOneFill()
  {
    int[] words = {0xFFFFFFFE, 0x42000004};

    ImmutableConciseSet res = ImmutableConciseSet.compact(new ImmutableConciseSet(IntBuffer.wrap(words)));
    ImmutableConciseSet.WordIterator itr = res.newWordIterator();

    Assert.assertEquals(0xFFFFFFFE, itr.next());
    Assert.assertEquals(0x42000004, itr.next());
    Assert.assertEquals(itr.hasNext(), false);
  }

  @Test
  public void testCompactTwoLiterals()
  {
    int[] words = {0xFFFFFFFE, 0xFFEFFEFF};

    ImmutableConciseSet res = ImmutableConciseSet.compact(new ImmutableConciseSet(IntBuffer.wrap(words)));
    ImmutableConciseSet.WordIterator itr = res.newWordIterator();

    Assert.assertEquals(0xFFFFFFFE, itr.next());
    Assert.assertEquals(0xFFEFFEFF, itr.next());
    Assert.assertEquals(itr.hasNext(), false);
  }

  /**
   * Set 1: zero literal, zero fill with flipped bit 33, literal
   * Set 2: zero literal, zero fill with flipped bit 34, literal
   * <p/>
   * Testing merge
   */
  @Test
  public void testUnion1()
  {
    final int[] ints1 = {33, 100000};
    final int[] ints2 = {34, 100000};
    List<Integer> expected = Arrays.asList(33, 34, 100000);

    ConciseSet set1 = new ConciseSet();
    for (int i : ints1) {
      set1.add(i);
    }
    ConciseSet set2 = new ConciseSet();
    for (int i : ints2) {
      set2.add(i);
    }
    List<ImmutableConciseSet> sets = Arrays.asList(
        ImmutableConciseSet.newImmutableFromMutable(set1),
        ImmutableConciseSet.newImmutableFromMutable(set2)
    );

    verifyUnion(expected, sets);
  }

  /**
   * Set 1: zero literal, zero fill with flipped bit 33, literal
   * Set 2: zero literal, zero fill with flipped bit 34, literal
   * <p/>
   * Testing merge
   */
  @Test
  public void testUnion2()
  {
    final int[] ints1 = {33, 100000};
    final int[] ints2 = {34, 200000};
    List<Integer> expected = Arrays.asList(33, 34, 100000, 200000);

    ConciseSet set1 = new ConciseSet();
    for (int i : ints1) {
      set1.add(i);
    }
    ConciseSet set2 = new ConciseSet();
    for (int i : ints2) {
      set2.add(i);
    }
    List<ImmutableConciseSet> sets = Arrays.asList(
        ImmutableConciseSet.newImmutableFromMutable(set1),
        ImmutableConciseSet.newImmutableFromMutable(set2)
    );

    verifyUnion(expected, sets);
  }

  /**
   * Set 1: zero fill, one fill
   * Set 2: zero fill, one fill with flipped bit 62
   * <p/>
   * Testing merge
   */
  @Test
  public void testUnion3()
  {
    List<Integer> expected = Lists.newArrayList();
    ConciseSet set1 = new ConciseSet();
    for (int i = 62; i < 10001; i++) {
      set1.add(i);
    }
    ConciseSet set2 = new ConciseSet();
    for (int i = 63; i < 10002; i++) {
      set2.add(i);
    }
    List<ImmutableConciseSet> sets = Arrays.asList(
        ImmutableConciseSet.newImmutableFromMutable(set1),
        ImmutableConciseSet.newImmutableFromMutable(set2)
    );

    for (int i = 62; i < 10002; i++) {
      expected.add(i);
    }

    verifyUnion(expected, sets);
  }

  /**
   * Set 1: zero literal, one fill with flipped bit 62
   * Set 2: zero literal, literal, one fill, literal
   * <p/>
   * Testing merge
   */
  @Test
  public void testUnion4()
  {
    List<Integer> expected = Lists.newArrayList();
    ConciseSet set1 = new ConciseSet();
    for (int i = 63; i < 1001; i++) {
      set1.add(i);
    }
    ConciseSet set2 = new ConciseSet();
    for (int i = 64; i < 1002; i++) {
      set2.add(i);
    }
    List<ImmutableConciseSet> sets = Arrays.asList(
        ImmutableConciseSet.newImmutableFromMutable(set1),
        ImmutableConciseSet.newImmutableFromMutable(set2)
    );

    for (int i = 63; i < 1002; i++) {
      expected.add(i);
    }


    ConciseSet blah = new ConciseSet();
    for (int i : expected) {
      blah.add(i);
    }
    verifyUnion(expected, sets);
  }

  /**
   * Set 1: literal
   * Set 2: zero fill, zero literal, zero fill with flipped 33 bit, zero fill with flipped 1000000 bit, literal
   * Set3: literal, zero fill with flipped 34th bit, literal
   * <p/>
   * Testing merge
   */
  @Test
  public void testUnion5()
  {
    final int[] ints1 = {1, 2, 3, 4, 5};
    final int[] ints2 = {100000, 2405983, 33};
    final int[] ints3 = {0, 4, 5, 34, 333333};
    final List<Integer> expected = Arrays.asList(0, 1, 2, 3, 4, 5, 33, 34, 100000, 333333, 2405983);

    ConciseSet set1 = new ConciseSet();
    for (int i : ints1) {
      set1.add(i);
    }
    ConciseSet set2 = new ConciseSet();
    for (int i : ints2) {
      set2.add(i);
    }
    ConciseSet set3 = new ConciseSet();
    for (int i : ints3) {
      set3.add(i);
    }

    List<ImmutableConciseSet> sets = Arrays.asList(
        ImmutableConciseSet.newImmutableFromMutable(set1),
        ImmutableConciseSet.newImmutableFromMutable(set2),
        ImmutableConciseSet.newImmutableFromMutable(set3)
    );

    verifyUnion(expected, sets);
  }

  /**
   * Set 1: literal
   * Set 2: literal
   * <p/>
   * Testing merge
   */
  @Test
  public void testUnion6()
  {
    List<Integer> expected = Lists.newArrayList();
    ConciseSet set1 = new ConciseSet();
    for (int i = 0; i < 30; i++) {
      if (i != 28) {
        set1.add(i);
      }
    }
    ConciseSet set2 = new ConciseSet();
    for (int i = 0; i < 30; i++) {
      if (i != 27) {
        set2.add(i);
      }
    }
    List<ImmutableConciseSet> sets = Arrays.asList(
        ImmutableConciseSet.newImmutableFromMutable(set1),
        ImmutableConciseSet.newImmutableFromMutable(set2)
    );

    for (int i = 0; i < 30; i++) {
      expected.add(i);
    }

    verifyUnion(expected, sets);
  }

  /**
   * Set 1: zero literal, literal, one fill with flipped bit
   * Set 2: zero literal, one fill with flipped bit
   * <p/>
   * Testing merge
   */
  @Test
  public void testUnion7()
  {
    List<Integer> expected = Lists.newArrayList();
    ConciseSet set1 = new ConciseSet();
    for (int i = 64; i < 1005; i++) {
      set1.add(i);
    }
    ConciseSet set2 = new ConciseSet();
    for (int i = 63; i < 99; i++) {
      set2.add(i);
    }
    List<ImmutableConciseSet> sets = Arrays.asList(
        ImmutableConciseSet.newImmutableFromMutable(set1),
        ImmutableConciseSet.newImmutableFromMutable(set2)
    );

    for (int i = 63; i < 1005; i++) {
      expected.add(i);
    }

    verifyUnion(expected, sets);
  }

  /**
   * Set 1: One fill with flipped 27th bit
   * Set 2: One fill with flipped 28th bit
   * <p/>
   * Testing creation of one fill with no flipped bits
   */
  @Test
  public void testUnion8()
  {
    List<Integer> expected = Lists.newArrayList();
    ConciseSet set1 = new ConciseSet();
    for (int i = 0; i < 1000; i++) {
      if (i != 27) {
        set1.add(i);
      }
    }
    ConciseSet set2 = new ConciseSet();
    for (int i = 0; i < 1000; i++) {
      if (i != 28) {
        set2.add(i);
      }
    }
    List<ImmutableConciseSet> sets = Arrays.asList(
        ImmutableConciseSet.newImmutableFromMutable(set1),
        ImmutableConciseSet.newImmutableFromMutable(set2)
    );

    for (int i = 0; i < 1000; i++) {
      expected.add(i);
    }

    verifyUnion(expected, sets);
  }

  /**
   * Set 1: Literal and one fill
   * Set 2: One fill with flipped 28th bit
   * <p/>
   * Testing creation of one fill with correct flipped bit
   */
  @Test
  public void testUnion9()
  {
    List<Integer> expected = Lists.newArrayList();
    ConciseSet set1 = new ConciseSet();
    for (int i = 0; i < 1000; i++) {
      if (!(i == 27 || i == 28)) {
        set1.add(i);
      }
    }
    ConciseSet set2 = new ConciseSet();
    for (int i = 0; i < 1000; i++) {
      if (i != 28) {
        set2.add(i);
      }
    }
    List<ImmutableConciseSet> sets = Arrays.asList(
        ImmutableConciseSet.newImmutableFromMutable(set1),
        ImmutableConciseSet.newImmutableFromMutable(set2)
    );

    for (int i = 0; i < 1000; i++) {
      if (i != 28) {
        expected.add(i);
      }
    }

    verifyUnion(expected, sets);
  }

  /**
   * Set 1: Multiple literals
   * Set 2: Multiple literals
   * <p/>
   * Testing merge of pure sequences of literals
   */
  @Test
  public void testUnion10()
  {
    List<Integer> expected = Lists.newArrayList();
    ConciseSet set1 = new ConciseSet();
    for (int i = 0; i < 1000; i += 2) {
      set1.add(i);
    }
    ConciseSet set2 = new ConciseSet();
    for (int i = 1; i < 1000; i += 2) {
      set2.add(i);
    }
    List<ImmutableConciseSet> sets = Arrays.asList(
        ImmutableConciseSet.newImmutableFromMutable(set1),
        ImmutableConciseSet.newImmutableFromMutable(set2)
    );

    for (int i = 0; i < 1000; i++) {
      expected.add(i);
    }

    verifyUnion(expected, sets);
  }

  /**
   * Set 1: Multiple literals
   * Set 2: Zero fill and literal
   * <p/>
   * Testing skipping of zero fills
   */
  @Test
  public void testUnion11()
  {
    List<Integer> expected = Lists.newArrayList();
    ConciseSet set1 = new ConciseSet();
    for (int i = 0; i < 1000; i += 2) {
      set1.add(i);
    }
    ConciseSet set2 = new ConciseSet();
    set2.add(10000);

    List<ImmutableConciseSet> sets = Arrays.asList(
        ImmutableConciseSet.newImmutableFromMutable(set1),
        ImmutableConciseSet.newImmutableFromMutable(set2)
    );

    for (int i = 0; i < 1000; i += 2) {
      expected.add(i);
    }
    expected.add(10000);

    verifyUnion(expected, sets);
  }

  /**
   * Set 1: Literal with 4 bits marked
   * Set 2: Zero fill with flipped bit 5
   * <p/>
   * Testing merge of literal and zero fill with flipped bit
   */
  @Test
  public void testUnion12()
  {
    final int[] ints1 = {1, 2, 3, 4};
    final int[] ints2 = {5, 1000};
    final List<Integer> expected = Arrays.asList(1, 2, 3, 4, 5, 1000);

    ConciseSet set1 = new ConciseSet();
    for (int i : ints1) {
      set1.add(i);
    }
    ConciseSet set2 = new ConciseSet();
    for (int i : ints2) {
      set2.add(i);
    }

    List<ImmutableConciseSet> sets = Arrays.asList(
        ImmutableConciseSet.newImmutableFromMutable(set1),
        ImmutableConciseSet.newImmutableFromMutable(set2)
    );

    verifyUnion(expected, sets);
  }

  /**
   * Set 1: Literal with bit 0
   * Set 2: One fill with flipped bit 0
   * <p/>
   * Testing merge of literal and one fill with flipped bit
   */
  @Test
  public void testUnion13()
  {
    List<Integer> expected = Lists.newArrayList();
    final int[] ints1 = {0};

    ConciseSet set1 = new ConciseSet();
    for (int i : ints1) {
      set1.add(i);
    }
    ConciseSet set2 = new ConciseSet();
    for (int i = 1; i < 100; i++) {
      set2.add(i);
    }

    List<ImmutableConciseSet> sets = Arrays.asList(
        ImmutableConciseSet.newImmutableFromMutable(set1),
        ImmutableConciseSet.newImmutableFromMutable(set2)
    );

    for (int i = 0; i < 100; i++) {
      expected.add(i);
    }

    verifyUnion(expected, sets);
  }

  /**
   * Set 1: Zero fill with flipped bit 0
   * Set 2: One fill with flipped bit 0
   * <p/>
   * Testing merge of flipped bits in zero and one fills
   */
  @Test
  public void testUnion14()
  {
    List<Integer> expected = Lists.newArrayList();
    final int[] ints1 = {0, 100};

    ConciseSet set1 = new ConciseSet();
    for (int i : ints1) {
      set1.add(i);
    }
    ConciseSet set2 = new ConciseSet();
    for (int i = 1; i < 100; i++) {
      set2.add(i);
    }

    List<ImmutableConciseSet> sets = Arrays.asList(
        ImmutableConciseSet.newImmutableFromMutable(set1),
        ImmutableConciseSet.newImmutableFromMutable(set2)
    );

    for (int i = 0; i <= 100; i++) {
      expected.add(i);
    }

    verifyUnion(expected, sets);
  }

  /**
   * Set 1: Zero fill with flipped bit 1
   * Set 2: Literal with 0th bit marked
   * Set 3: One Fill from 1 to 100 with flipped bit 0
   * <p/>
   * Testing merge of flipped bits in zero and one fills with a literal
   */
  @Test
  public void testUnion15()
  {
    List<Integer> expected = Lists.newArrayList();
    final int[] ints1 = {1, 100};
    final int[] ints2 = {0};

    ConciseSet set1 = new ConciseSet();
    for (int i : ints1) {
      set1.add(i);
    }
    ConciseSet set2 = new ConciseSet();
    for (int i : ints2) {
      set2.add(i);
    }
    ConciseSet set3 = new ConciseSet();
    for (int i = 1; i < 100; i++) {
      set3.add(i);
    }

    List<ImmutableConciseSet> sets = Arrays.asList(
        ImmutableConciseSet.newImmutableFromMutable(set1),
        ImmutableConciseSet.newImmutableFromMutable(set2),
        ImmutableConciseSet.newImmutableFromMutable(set3)
    );

    for (int i = 0; i <= 100; i++) {
      expected.add(i);
    }

    verifyUnion(expected, sets);
  }

  /**
   * Testing merge of offset elements
   */
  @Test
  public void testUnion16()
  {
    final int[] ints1 = {1001, 1002, 1003};
    final int[] ints2 = {1034, 1035, 1036};
    List expected = Arrays.asList(1001, 1002, 1003, 1034, 1035, 1036);

    ConciseSet set1 = new ConciseSet();
    for (int i : ints1) {
      set1.add(i);
    }
    ConciseSet set2 = new ConciseSet();
    for (int i : ints2) {
      set2.add(i);
    }

    List<ImmutableConciseSet> sets = Arrays.asList(
        ImmutableConciseSet.newImmutableFromMutable(set1),
        ImmutableConciseSet.newImmutableFromMutable(set2)
    );

    verifyUnion(expected, sets);
  }

  /**
   * Testing merge of same elements
   */
  @Test
  public void testUnion17()
  {
    final int[] ints1 = {1, 2, 3, 4, 5};
    final int[] ints2 = {1, 2, 3, 4, 5};
    List expected = Arrays.asList(1, 2, 3, 4, 5);

    ConciseSet set1 = new ConciseSet();
    for (int i : ints1) {
      set1.add(i);
    }
    ConciseSet set2 = new ConciseSet();
    for (int i : ints2) {
      set2.add(i);
    }

    List<ImmutableConciseSet> sets = Arrays.asList(
        ImmutableConciseSet.newImmutableFromMutable(set1),
        ImmutableConciseSet.newImmutableFromMutable(set2)
    );

    verifyUnion(expected, sets);
  }

  @Test
  public void testUnion18()
  {
    List<Integer> expected = Lists.newArrayList();
    ConciseSet set1 = new ConciseSet();
    for (int i = 0; i < 1000; i++) {
      set1.add(i);
    }
    ConciseSet set2 = new ConciseSet();
    set2.add(1000);
    set2.add(10000);

    List<ImmutableConciseSet> sets = Arrays.asList(
        ImmutableConciseSet.newImmutableFromMutable(set1),
        ImmutableConciseSet.newImmutableFromMutable(set2)
    );

    for (int i = 0; i < 1001; i++) {
      expected.add(i);
    }
    expected.add(10000);

    verifyUnion(expected, sets);
  }

  /**
   * Set 1: one fill, all ones literal
   * Set 2: zero fill, one fill, literal
   */
  @Test
  public void testUnion19()
  {
    List<Integer> expected = Lists.newArrayList();
    ConciseSet set1 = new ConciseSet();
    for (int i = 0; i < 93; i++) {
      set1.add(i);
    }
    ConciseSet set2 = new ConciseSet();
    for (int i = 62; i < 1000; i++) {
      set2.add(i);
    }

    List<ImmutableConciseSet> sets = Arrays.asList(
        ImmutableConciseSet.newImmutableFromMutable(set1),
        ImmutableConciseSet.newImmutableFromMutable(set2)
    );

    for (int i = 0; i < 1000; i++) {
      expected.add(i);
    }

    verifyUnion(expected, sets);
  }

  /**
   * Set 1: literal, one fill, literal
   * Set 2: zero fill, literal that falls within the one fill above, one fill that falls in one fill above, one fill
   */
  @Test
  public void testUnion20()
  {
    List<Integer> expected = Lists.newArrayList();
    ConciseSet set1 = new ConciseSet();
    for (int i = 0; i < 5; i++) {
      set1.add(i);
    }
    for (int i = 31; i < 1000; i++) {
      set1.add(i);
    }

    ConciseSet set2 = new ConciseSet();
    for (int i = 62; i < 68; i++) {
      set2.add(i);
    }
    for (int i = 800; i < 1000; i++) {
      set2.add(i);
    }

    List<ImmutableConciseSet> sets = Arrays.asList(
        ImmutableConciseSet.newImmutableFromMutable(set1),
        ImmutableConciseSet.newImmutableFromMutable(set2)
    );

    for (int i = 0; i < 5; i++) {
      expected.add(i);
    }
    for (int i = 31; i < 1000; i++) {
      expected.add(i);
    }

    verifyUnion(expected, sets);
  }

  @Test
  public void testUnion21()
  {
    ConciseSet set1 = new ConciseSet();
    for (int i = 32; i < 93; i++) {
      set1.add(i);
    }

    ConciseSet set2 = new ConciseSet();
    for (int i = 0; i < 62; i++) {
      set2.add(i);
    }

    List<ImmutableConciseSet> sets = Arrays.asList(
        ImmutableConciseSet.newImmutableFromMutable(set1),
        ImmutableConciseSet.newImmutableFromMutable(set2)
    );

    List<Integer> expected = Lists.newArrayList();
    for (int i = 0; i < 93; i++) {
      expected.add(i);
    }

    verifyUnion(expected, sets);
  }

  @Test
  public void testUnion22()
  {
    ConciseSet set1 = new ConciseSet();
    for (int i = 93; i < 1000; i++) {
      set1.add(i);
    }

    ConciseSet set2 = new ConciseSet();
    for (int i = 0; i < 32; i++) {
      set2.add(i);
    }

    List<ImmutableConciseSet> sets = Arrays.asList(
        ImmutableConciseSet.newImmutableFromMutable(set1),
        ImmutableConciseSet.newImmutableFromMutable(set2)
    );

    List<Integer> expected = Lists.newArrayList();
    for (int i = 0; i < 32; i++) {
      expected.add(i);
    }
    for (int i = 93; i < 1000; i++) {
      expected.add(i);
    }

    verifyUnion(expected, sets);
  }

  @Test
  public void testUnion23()
  {
    ConciseSet set1 = new ConciseSet();
    set1.add(10);
    set1.add(1000);

    ConciseSet set2 = new ConciseSet();
    for (int i = 0; i < 10; i++) {
      set2.add(i);
    }
    for (int i = 11; i < 1000; i++) {
      set2.add(i);
    }

    List<ImmutableConciseSet> sets = Arrays.asList(
        ImmutableConciseSet.compact(ImmutableConciseSet.newImmutableFromMutable(set1)),
        ImmutableConciseSet.compact(ImmutableConciseSet.newImmutableFromMutable(set2))
    );
    List<Integer> expected = new ArrayList<>();
    for (int i = 0; i <= 1000; i++) {
      expected.add(i);
    }

    verifyUnion(expected, sets);
  }

  private void verifyUnion(List<Integer> expected, List<ImmutableConciseSet> sets)
  {
    List<Integer> actual = Lists.newArrayList();
    ImmutableConciseSet set = ImmutableConciseSet.union(sets);
    IntSet.IntIterator itr = set.iterator();
    while (itr.hasNext()) {
      actual.add(itr.next());
    }
    Assert.assertEquals(expected, actual);
  }

  /**
   * Basic complement with no length
   */
  @Test
  public void testComplement1()
  {
    final int[] ints = {1, 100};
    List<Integer> expected = Lists.newArrayList();

    ConciseSet set = new ConciseSet();
    for (int i : ints) {
      set.add(i);
    }

    for (int i = 0; i <= 100; i++) {
      if (i != 1 && i != 100) {
        expected.add(i);
      }
    }

    ImmutableConciseSet testSet = ImmutableConciseSet.newImmutableFromMutable(set);

    verifyComplement(expected, testSet, NO_COMPLEMENT_LENGTH);
  }

  /**
   * Complement of a single partial word
   */
  @Test
  public void testComplement2()
  {
    List<Integer> expected = Lists.newArrayList();

    ConciseSet set = new ConciseSet();
    for (int i = 0; i < 15; i++) {
      set.add(i);
    }

    ImmutableConciseSet testSet = ImmutableConciseSet.newImmutableFromMutable(set);

    verifyComplement(expected, testSet, NO_COMPLEMENT_LENGTH);
  }

  /**
   * Complement of a single partial word with a length set in the same word
   */
  @Test
  public void testComplement3()
  {
    List<Integer> expected = Lists.newArrayList();
    final int length = 21;

    ConciseSet set = new ConciseSet();
    for (int i = 0; i < 15; i++) {
      set.add(i);
    }
    for (int i = 15; i < length; i++) {
      expected.add(i);
    }

    ImmutableConciseSet testSet = ImmutableConciseSet.newImmutableFromMutable(set);

    verifyComplement(expected, testSet, length);
  }

  /**
   * Complement of a single partial word with a length set in a different word
   */
  @Test
  public void testComplement4()
  {
    List<Integer> expected = Lists.newArrayList();
    final int length = 41;

    ConciseSet set = new ConciseSet();
    for (int i = 0; i < 15; i++) {
      set.add(i);
    }
    for (int i = 15; i < length; i++) {
      expected.add(i);
    }

    ImmutableConciseSet testSet = ImmutableConciseSet.newImmutableFromMutable(set);

    verifyComplement(expected, testSet, length);
  }

  /**
   * Complement of a single partial word with a length set to create a one fill
   */
  @Test
  public void testComplement5()
  {
    List<Integer> expected = Lists.newArrayList();
    final int length = 1001;

    ConciseSet set = new ConciseSet();
    for (int i = 0; i < 15; i++) {
      set.add(i);
    }
    for (int i = 15; i < length; i++) {
      expected.add(i);
    }

    ImmutableConciseSet testSet = ImmutableConciseSet.newImmutableFromMutable(set);

    verifyComplement(expected, testSet, length);
  }

  /**
   * Complement of words with a length set to create a one fill
   */
  @Test
  public void testComplement6()
  {
    List<Integer> expected = Lists.newArrayList();
    final int length = 1001;

    ConciseSet set = new ConciseSet();
    for (int i = 65; i <= 100; i++) {
      set.add(i);
    }
    for (int i = 0; i < length; i++) {
      if (i < 65 || i > 100) {
        expected.add(i);
      }
    }

    ImmutableConciseSet testSet = ImmutableConciseSet.newImmutableFromMutable(set);

    verifyComplement(expected, testSet, length);
  }

  /**
   * Complement of 2 words with a length in the second word
   */
  @Test
  public void testComplement7()
  {
    List<Integer> expected = Lists.newArrayList();
    final int length = 37;

    ConciseSet set = new ConciseSet();
    for (int i = 0; i <= 35; i++) {
      set.add(i);
    }
    expected.add(36);

    ImmutableConciseSet testSet = ImmutableConciseSet.newImmutableFromMutable(set);

    verifyComplement(expected, testSet, length);
  }

  /**
   * Complement of a one literal with a length set to complement the next bit in the next word
   */
  @Test
  public void testComplement8()
  {
    List<Integer> expected = Lists.newArrayList();
    final int length = 32;

    ConciseSet set = new ConciseSet();
    for (int i = 0; i <= 30; i++) {
      set.add(i);
    }
    expected.add(31);

    ImmutableConciseSet testSet = ImmutableConciseSet.newImmutableFromMutable(set);

    verifyComplement(expected, testSet, length);
  }

  /**
   * Complement of a null set with a length
   */
  @Test
  public void testComplement9()
  {
    final List<Integer> lengths = new ArrayList<Integer>();
    lengths.addAll(
        Arrays.asList(
            35,
            31,
            32,
            1,
            0,
            31 * 3,
            1024,
            ConciseSetUtils.MAX_ALLOWED_INTEGER
        )
    );
    final Random random = new Random(701534702L);
    for (int i = 0; i < 5; ++i) {
      lengths.add(random.nextInt(ConciseSetUtils.MAX_ALLOWED_INTEGER + 1));
    }
    final ImmutableConciseSet emptySet = new ImmutableConciseSet();
    for (final int length : lengths) {
      final ImmutableConciseSet complement = ImmutableConciseSet.complement(emptySet, length);
      final IntSet.IntIterator intIterator = complement.iterator();
      for (int i = 0; i < length; i++) {
        final int n = intIterator.next();
        if (i != n) {
          Assert.assertEquals(StringUtils.format("Failure at bit [%d] on length [%d]", i, length), i, n);
        }
      }
      NoSuchElementException ex = null;
      try {
        intIterator.next();
      }
      catch (NoSuchElementException e) {
        ex = e;
      }
      Assert.assertNotNull(ex);
    }
  }

  /**
   * Complement of a null set to create a one fill
   */
  @Test
  public void testComplement10()
  {
    List<Integer> expected = Lists.newArrayList();
    final int length = 93;

    for (int i = 0; i < length; i++) {
      expected.add(i);
    }

    ImmutableConciseSet testSet = new ImmutableConciseSet();

    verifyComplement(expected, testSet, length);
  }

  /**
   * Complement with correct last index
   */
  @Test
  public void testComplement11()
  {
    List<Integer> expected = Lists.newArrayList();
    int length = 18930;
    for (int i = 0; i < 500; i++) {
      expected.add(i);
    }
    for (int i = 18881; i < length; i++) {
      expected.add(i);
    }

    ConciseSet set = new ConciseSet();
    for (int i = 500; i <= 18880; i++) {
      set.add(i);
    }
    ImmutableConciseSet testSet = ImmutableConciseSet.newImmutableFromMutable(set);

    verifyComplement(expected, testSet, length);
  }

  /**
   * Complement with empty set and length in first block
   */
  @Test
  public void testComplement12()
  {
    List<Integer> expected = Lists.newArrayList();
    int length = 10;
    for (int i = 0; i < 10; i++) {
      expected.add(i);
    }

    ImmutableConciseSet testSet = new ImmutableConciseSet();

    verifyComplement(expected, testSet, length);
  }

  /**
   * Complement with empty list of some length
   */
  @Test
  public void testComplement13()
  {
    List<Integer> expected = Lists.newArrayList();
    int length = 10;
    for (int i = 0; i < length; i++) {
      expected.add(i);
    }
    ImmutableConciseSet testSet = new ImmutableConciseSet();

    verifyComplement(expected, testSet, length);
  }

  private void verifyComplement(List<Integer> expected, ImmutableConciseSet set, int endIndex)
  {
    List<Integer> actual = Lists.newArrayList();

    ImmutableConciseSet res;
    if (endIndex == NO_COMPLEMENT_LENGTH) {
      res = ImmutableConciseSet.complement(set);
    } else {
      res = ImmutableConciseSet.complement(set, endIndex);
    }

    IntSet.IntIterator itr = res.iterator();
    while (itr.hasNext()) {
      actual.add(itr.next());
    }
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testContains()
  {
    final ConciseSet conciseSet = new ConciseSet();
    final Random random = new Random(543167436715430L);
    final Set<Integer> integerSet = new HashSet<>();
    int max = -1;
    for (int i = 0; i < 100; ++i) {
      final int j = random.nextInt(1 << 20);
      integerSet.add(j);
      conciseSet.add(j);
      if (j > max) {
        max = j;
      }
    }
    final ImmutableConciseSet immutableConciseSet = ImmutableConciseSet.newImmutableFromMutable(conciseSet);
    for (int i = 0; i < max + 10; ++i) {
      final String s = Integer.toString(i);
      Assert.assertEquals(s, integerSet.contains(i), conciseSet.contains(i));
      Assert.assertEquals(s, integerSet.contains(i), immutableConciseSet.contains(i));
    }
  }
}
