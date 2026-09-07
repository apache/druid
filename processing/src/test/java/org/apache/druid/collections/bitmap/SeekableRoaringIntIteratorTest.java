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

package org.apache.druid.collections.bitmap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.buffer.DruidRoaringBufferAccess;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MappeableArrayContainer;
import org.roaringbitmap.buffer.MappeableBitmapContainer;
import org.roaringbitmap.buffer.MappeableRunContainer;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.roaringbitmap.buffer.PointableRoaringArray;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Checks {@link SeekableRoaringIntIterator} against {@link ImmutableRoaringBitmap#getIntIterator()}.
 *
 * <p>Bitmap values are unsigned, so this test carries them around as longs in {@code [0, 0xFFFFFFFF]} and uses
 * {@link #NONE} for "the iterator is exhausted".
 */
public class SeekableRoaringIntIteratorTest
{
  /**
   * Stands in for "no value", since every int is a legal bitmap value.
   */
  private static final long NONE = -1L;
  private static final long UNSIGNED_LIMIT = 1L << 32;
  private static final long RANDOM_SEED = 1234;

  public static Stream<Arguments> bitmaps()
  {
    final List<Arguments> cases = new ArrayList<>();
    final Random random = new Random(RANDOM_SEED);

    // Sparse bitmaps. Should end up as array containers.
    final MutableRoaringBitmap sparse = new MutableRoaringBitmap();
    for (int i = 0; i < 3_000_000; i += 1 + random.nextInt(4000)) {
      sparse.add(i);
    }
    cases.add(Arguments.of("sparse", toBufferBackedBitmap(sparse)));

    // Dense bitmaps. Should end up as bitmap containers.
    final MutableRoaringBitmap dense = new MutableRoaringBitmap();
    for (int i = 0; i < 1_000_000; i++) {
      if (random.nextDouble() < 0.6) {
        dense.add(i);
      }
    }
    cases.add(Arguments.of("dense", toBufferBackedBitmap(dense)));

    // Runs, with whole containers missing in between. Should end up as run containers.
    final MutableRoaringBitmap runs = new MutableRoaringBitmap();
    runs.add(200_000L, 400_000L);
    runs.add(2_000_000L, 2_300_000L);
    runs.add(9_000_000L, 9_010_000L);
    cases.add(Arguments.of("runs", toBufferBackedBitmap(runs)));

    // A single value far from the origin.
    final MutableRoaringBitmap lonely = new MutableRoaringBitmap();
    lonely.add(15_000_000);
    cases.add(Arguments.of("lonely", toBufferBackedBitmap(lonely)));

    // Backed by a MutableRoaringArray rather than ByteBuffer, which is what
    // WrappedRoaringBitmap.toImmutableBitmap() hands out.
    final MutableRoaringBitmap heap = new MutableRoaringBitmap();
    heap.add(50L, 70L);
    heap.add(300_000L, 305_000L);
    heap.add(5_000_000L, 5_000_100L);
    heap.runOptimize();
    cases.add(Arguments.of("heap", heap.toImmutableRoaringBitmap()));

    // All three kinds of distributions in one bitmap.
    final MutableRoaringBitmap mixed = new MutableRoaringBitmap();
    for (int i = 0; i < 100; i++) {
      mixed.add(i * 37);
    }
    for (int i = 0; i < 65536; i++) {
      if (random.nextDouble() < 0.5) {
        mixed.add(65536 + i);
      }
    }
    mixed.add(2 * 65536L, 3 * 65536L);
    mixed.add(4 * 65536 + 11);
    mixed.add(4 * 65536 + 65535);
    cases.add(Arguments.of("mixed", toBufferBackedBitmap(mixed)));

    // Consecutive container keys.
    final MutableRoaringBitmap adjacent = new MutableRoaringBitmap();
    for (int key = 0; key < 10; key++) {
      for (int i = 0; i < 20; i++) {
        adjacent.add(key * 65536 + i * 3000);
      }
    }
    cases.add(Arguments.of("adjacent", toBufferBackedBitmap(adjacent)));

    // The first and last value of several containers, so seeks land exactly on container edges.
    final MutableRoaringBitmap boundaries = new MutableRoaringBitmap();
    for (int key = 0; key < 5; key++) {
      boundaries.add(key * 65536);
      boundaries.add(key * 65536 + 65535);
    }
    cases.add(Arguments.of("boundaries", toBufferBackedBitmap(boundaries)));

    // Values above Integer.MAX_VALUE, where the container key has its high bit set and hs is negative.
    final MutableRoaringBitmap high = new MutableRoaringBitmap();
    high.add(1);
    high.add(0x7FFFFFFF);
    high.add(0x80000000);
    high.add(0x80000001);
    high.add(0xC0000000L, 0xC0010000L);
    high.add(0xFFFF0000L, 0xFFFF0010L);
    high.add(-1); // 0xFFFFFFFF, the largest unsigned value
    cases.add(Arguments.of("high", toBufferBackedBitmap(high)));

    return cases.stream();
  }

  @Test
  public void testFixturesCoverEveryContainerKind()
  {
    // Verify we really do have all three kinds of containers in the test bitmaps.
    final Set<String> kinds = new HashSet<>();
    bitmaps().forEach(args -> {
      final ImmutableRoaringBitmap bitmap = (ImmutableRoaringBitmap) args.get()[1];
      final PointableRoaringArray containers = DruidRoaringBufferAccess.highLowContainer(bitmap);
      for (int i = 0; i < containers.size(); i++) {
        kinds.add(containers.getContainerAtIndex(i).getClass().getSimpleName());
      }
    });

    assertTrue(kinds.contains(MappeableArrayContainer.class.getSimpleName()), "array containers, got " + kinds);
    assertTrue(kinds.contains(MappeableBitmapContainer.class.getSimpleName()), "bitmap containers, got " + kinds);
    assertTrue(kinds.contains(MappeableRunContainer.class.getSimpleName()), "run containers, got " + kinds);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("bitmaps")
  public void testSequentialIteration(final String name, final ImmutableRoaringBitmap bitmap)
  {
    final SeekableRoaringIntIterator iterator = new SeekableRoaringIntIterator(bitmap);
    final IntIterator reference = bitmap.getIntIterator();
    long n = 0;
    while (reference.hasNext()) {
      assertTrue(iterator.hasNext(), "ran out early at " + n);
      assertEquals(reference.next(), iterator.next(), "value " + n);
      n++;
    }
    assertFalse(iterator.hasNext(), "extra values after " + n);
    assertThrows(NoSuchElementException.class, iterator::next);
    assertThrows(NoSuchElementException.class, iterator::peekNext);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("bitmaps")
  public void testPeekNextDoesNotAdvance(final String name, final ImmutableRoaringBitmap bitmap)
  {
    final SeekableRoaringIntIterator iterator = new SeekableRoaringIntIterator(bitmap);
    final IntIterator reference = bitmap.getIntIterator();
    while (reference.hasNext()) {
      final int value = reference.next();
      assertEquals(value, iterator.peekNext());
      assertEquals(value, iterator.peekNext());
      assertEquals(value, iterator.next());
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("bitmaps")
  public void testRandomSeeksInBothDirections(final String name, final ImmutableRoaringBitmap bitmap)
  {
    final SeekableRoaringIntIterator iterator = new SeekableRoaringIntIterator(bitmap);
    final Random random = new Random(5678);
    final long span = span(bitmap);
    for (int trial = 0; trial < 200_000; trial++) {
      final int target = (int) random.nextLong(span);
      iterator.seek(target);
      assertEquals(expected(bitmap, target), peek(iterator), "seek(" + Integer.toUnsignedString(target) + ")");
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("bitmaps")
  public void testAscendingSeeks(final String name, final ImmutableRoaringBitmap bitmap)
  {
    final SeekableRoaringIntIterator iterator = new SeekableRoaringIntIterator(bitmap);
    final long span = span(bitmap);
    final long step = Math.max(1, span / 20_000);
    for (long target = 0; target < span; target += step) {
      iterator.seek((int) target);
      assertEquals(expected(bitmap, (int) target), peek(iterator), "seek(" + target + ")");
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("bitmaps")
  public void testDescendingSeeks(final String name, final ImmutableRoaringBitmap bitmap)
  {
    final SeekableRoaringIntIterator iterator = new SeekableRoaringIntIterator(bitmap);
    final long span = span(bitmap);
    final long step = Math.max(1, span / 20_000);
    for (long target = span - 1; target >= 0; target -= step) {
      iterator.seek((int) target);
      assertEquals(expected(bitmap, (int) target), peek(iterator), "seek(" + target + ")");
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("bitmaps")
  public void testSeekIsIdempotent(final String name, final ImmutableRoaringBitmap bitmap)
  {
    final SeekableRoaringIntIterator iterator = new SeekableRoaringIntIterator(bitmap);
    final Random random = new Random(2468);
    final long span = span(bitmap);
    for (int trial = 0; trial < 20_000; trial++) {
      final int target = (int) random.nextLong(span);
      iterator.seek(target);
      final long first = peek(iterator);
      iterator.seek(target);
      assertEquals(first, peek(iterator), "second seek(" + Integer.toUnsignedString(target) + ")");
      assertEquals(expected(bitmap, target), first, "seek(" + Integer.toUnsignedString(target) + ")");
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("bitmaps")
  public void testInterleavedNextAndAdvanceIfNeeded(final String name, final ImmutableRoaringBitmap bitmap)
  {
    final SeekableRoaringIntIterator iterator = new SeekableRoaringIntIterator(bitmap);
    final PeekableIntIterator reference = bitmap.getIntIterator();
    final Random random = new Random(4321);
    long cursor = 0;
    while (reference.hasNext() && iterator.hasNext()) {
      if (random.nextBoolean()) {
        assertEquals(reference.next(), iterator.next(), "next at " + cursor);
      } else {
        // advanceIfNeeded is forward only, so never ask for less than where the iterator already is.
        cursor = Math.max(cursor, peek(iterator)) + random.nextInt(20000);
        if (cursor >= UNSIGNED_LIMIT) {
          break;
        }
        reference.advanceIfNeeded((int) cursor);
        iterator.advanceIfNeeded((int) cursor);
        assertEquals(peek(reference), peek(iterator), "advanceIfNeeded(" + cursor + ")");
      }
    }
    assertEquals(reference.hasNext(), iterator.hasNext());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("bitmaps")
  public void testRandomOperationsMatchReference(final String name, final ImmutableRoaringBitmap bitmap)
  {
    final Random random = new Random(13579);
    final long span = span(bitmap);
    SeekableRoaringIntIterator iterator = new SeekableRoaringIntIterator(bitmap);
    PeekableIntIterator reference = bitmap.getIntIterator();

    for (int op = 0; op < 100_000; op++) {
      final int choice = random.nextInt(10);
      final String what;
      if (choice < 4) {
        what = "next";
        assertEquals(reference.hasNext(), iterator.hasNext(), "hasNext before " + what + " at op " + op);
        if (reference.hasNext()) {
          assertEquals(reference.next(), iterator.next(), "next at op " + op);
        } else {
          assertThrows(NoSuchElementException.class, iterator::next, "next at op " + op);
        }
      } else if (choice < 7) {
        // Forward only, per the advanceIfNeeded contract. An exhausted iterator accepts anything, since it must
        // stay exhausted either way.
        final long from = Math.max(0, peek(iterator));
        final long minval = from + random.nextLong(span - from);
        what = "advanceIfNeeded(" + minval + ")";
        reference.advanceIfNeeded((int) minval);
        iterator.advanceIfNeeded((int) minval);
      } else if (choice < 9) {
        final long target = random.nextLong(span);
        what = "seek(" + target + ")";
        iterator.seek((int) target);
        reference = bitmap.getIntIterator();
        reference.advanceIfNeeded((int) target);
      } else {
        what = "clone";
        iterator = iterator.clone();
        reference = reference.clone();
      }
      assertEquals(peek(reference), peek(iterator), what + " at op " + op);
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("bitmaps")
  public void testCloneIsIndependent(final String name, final ImmutableRoaringBitmap bitmap)
  {
    final SeekableRoaringIntIterator original = new SeekableRoaringIntIterator(bitmap);
    for (int i = 0; i < 100 && original.hasNext(); i++) {
      original.next();
    }
    final SeekableRoaringIntIterator copy = original.clone();
    assertEquals(original.hasNext(), copy.hasNext());
    while (original.hasNext()) {
      assertTrue(copy.hasNext());
      assertEquals(original.next(), copy.next());
    }
    assertFalse(copy.hasNext());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("bitmaps")
  public void testCloneDoesNotShareCursor(final String name, final ImmutableRoaringBitmap bitmap)
  {
    final SeekableRoaringIntIterator original = new SeekableRoaringIntIterator(bitmap);
    final long first = peek(original);

    // Draining the copy leaves the original untouched.
    final SeekableRoaringIntIterator copy = original.clone();
    while (copy.hasNext()) {
      copy.next();
    }
    assertFalse(copy.hasNext());
    assertEquals(first, peek(original));

    // And seeking the original leaves an earlier copy untouched.
    final SeekableRoaringIntIterator pinned = original.clone();
    final int last = bitmap.last();
    original.seek(last);
    assertEquals(Integer.toUnsignedLong(last), peek(original));
    assertEquals(first, peek(pinned));

    // A clone of an exhausted iterator is exhausted, and can be brought back on its own.
    original.next();
    assertFalse(original.hasNext());
    final SeekableRoaringIntIterator exhausted = original.clone();
    assertFalse(exhausted.hasNext());
    assertThrows(NoSuchElementException.class, exhausted::next);
    exhausted.seek(0);
    assertEquals(first, peek(exhausted));
    assertFalse(original.hasNext());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("bitmaps")
  public void testAdvanceIfNeededDoesNotMoveBackwards(final String name, final ImmutableRoaringBitmap bitmap)
  {
    final SeekableRoaringIntIterator iterator = new SeekableRoaringIntIterator(bitmap);
    final Random random = new Random(97531);

    for (int trial = 0; trial < 20_000 && iterator.hasNext(); trial++) {
      final long position = peek(iterator);
      iterator.advanceIfNeeded((int) random.nextLong(position + 1));
      assertEquals(position, peek(iterator), "backwards advanceIfNeeded from " + position);
      iterator.next();
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("bitmaps")
  public void testAdvanceIfNeededOnExhaustedIteratorIsANoOp(final String name, final ImmutableRoaringBitmap bitmap)
  {
    final SeekableRoaringIntIterator iterator = new SeekableRoaringIntIterator(bitmap);
    final int first = iterator.peekNext();
    while (iterator.hasNext()) {
      iterator.next();
    }

    final long span = span(bitmap);
    final Random random = new Random(11223);
    for (int trial = 0; trial < 1000; trial++) {
      iterator.advanceIfNeeded((int) random.nextLong(span));
      assertFalse(iterator.hasNext(), "still exhausted");
    }
    iterator.advanceIfNeeded(0);
    assertFalse(iterator.hasNext(), "still exhausted");

    // seek() is not bound by the forward-only rule, so it does come back.
    iterator.seek(0);
    assertEquals(first, iterator.peekNext());
  }

  @Test
  public void testAdvanceIntoGapLandsAtStartOfNextRange()
  {
    final MutableRoaringBitmap mutable = new MutableRoaringBitmap();
    mutable.add(2_000_000L, 2_200_000L);
    mutable.add(4_000_000L, 4_300_000L);
    final ImmutableRoaringBitmap bitmap = toBufferBackedBitmap(mutable);

    final SeekableRoaringIntIterator iterator = new SeekableRoaringIntIterator(bitmap);

    assertEquals(2_000_000, iterator.next());

    iterator.advanceIfNeeded(2_100_000);
    assertEquals(2_100_000, iterator.next());

    assertFalse(bitmap.contains(2_300_000));
    iterator.advanceIfNeeded(2_300_000);
    assertEquals(4_000_000, iterator.peekNext());

    iterator.advanceIfNeeded(4_000_000);
    assertEquals(4_000_000, iterator.next());
  }

  @Test
  public void testSeekIntoWholeContainerGap()
  {
    // Targets in gaps that span whole containers, reached from both directions.
    final MutableRoaringBitmap mutable = new MutableRoaringBitmap();
    mutable.add(5);
    mutable.add(10 * 65536 + 7);
    mutable.add(50 * 65536 + 9);
    final ImmutableRoaringBitmap bitmap = toBufferBackedBitmap(mutable);
    final SeekableRoaringIntIterator iterator = new SeekableRoaringIntIterator(bitmap);

    final int[] targets = {
        0,
        5,
        6,
        65536,
        3 * 65536,
        10 * 65536,
        10 * 65536 + 7,
        10 * 65536 + 8,
        11 * 65536,
        49 * 65536,
        50 * 65536 + 9,
        50 * 65536 + 10,
        60 * 65536
    };

    for (final int target : targets) {
      iterator.seek(target);
      assertEquals(expected(bitmap, target), peek(iterator), "ascending seek(" + target + ")");
    }
    for (int i = targets.length - 1; i >= 0; i--) {
      iterator.seek(targets[i]);
      assertEquals(expected(bitmap, targets[i]), peek(iterator), "descending seek(" + targets[i] + ")");
    }
  }

  @Test
  public void testEmptyBitmap()
  {
    final ImmutableRoaringBitmap bitmap = toBufferBackedBitmap(new MutableRoaringBitmap());
    final SeekableRoaringIntIterator iterator = new SeekableRoaringIntIterator(bitmap);
    assertFalse(iterator.hasNext());
    iterator.seek(0);
    assertFalse(iterator.hasNext());
    iterator.seek(1_000_000);
    assertFalse(iterator.hasNext());
    iterator.advanceIfNeeded(0);
    assertFalse(iterator.hasNext());
    assertThrows(NoSuchElementException.class, iterator::next);
    assertThrows(NoSuchElementException.class, iterator::peekNext);
    assertFalse(iterator.clone().hasNext());
  }

  @Test
  public void testSeekPastEndThenBack()
  {
    final MutableRoaringBitmap mutable = new MutableRoaringBitmap();
    mutable.add(100L, 200L);
    final ImmutableRoaringBitmap bitmap = toBufferBackedBitmap(mutable);
    final SeekableRoaringIntIterator iterator = new SeekableRoaringIntIterator(bitmap);

    iterator.seek(1_000_000);
    assertFalse(iterator.hasNext());

    iterator.seek(150);
    assertEquals(150, iterator.peekNext());

    iterator.seek(0);
    assertEquals(100, iterator.peekNext());
  }

  @Test
  public void testUnsignedValuesAboveIntegerMaxValue()
  {
    final MutableRoaringBitmap mutable = new MutableRoaringBitmap();
    mutable.add(1);
    mutable.add(0x7FFFFFFF);
    mutable.add(0x80000000);
    mutable.add(0xFFFFFFF0L, 0x100000000L);
    final ImmutableRoaringBitmap bitmap = toBufferBackedBitmap(mutable);
    final SeekableRoaringIntIterator iterator = new SeekableRoaringIntIterator(bitmap);

    assertEquals(1, iterator.next());
    assertEquals(0x7FFFFFFF, iterator.next());
    assertEquals(0x80000000, iterator.next());
    assertEquals(0xFFFFFFF0, iterator.next());

    // Forwards across the sign boundary.
    iterator.seek(0x7FFFFFFF);
    assertEquals(0x7FFFFFFF, iterator.peekNext());
    iterator.advanceIfNeeded(0x80000000);
    assertEquals(0x80000000, iterator.peekNext());

    // Backwards across it too, which advanceIfNeeded would refuse.
    iterator.seek(0);
    assertEquals(1, iterator.peekNext());

    // The very top of the range.
    iterator.seek(0xFFFFFFFF);
    assertEquals(0xFFFFFFFF, iterator.peekNext());
    assertEquals(0xFFFFFFFF, iterator.next());
    assertFalse(iterator.hasNext());

    // A gap between the sign boundary and the top containers.
    iterator.seek(0x90000000);
    assertEquals(0xFFFFFFF0, iterator.peekNext());
  }

  @Test
  public void testWrappedImmutableRoaringBitmapHandsOutSeekableIterator()
  {
    final MutableRoaringBitmap mutable = new MutableRoaringBitmap();
    mutable.add(1L, 100L);
    final ImmutableBitmap wrapped = new WrappedImmutableRoaringBitmap(toBufferBackedBitmap(mutable));
    assertInstanceOf(SeekableRoaringIntIterator.class, wrapped.peekableIterator());
  }

  /**
   * Serializes through a ByteBuffer so the bitmap is backed by an ImmutableRoaringArray.
   */
  private static ImmutableRoaringBitmap toBufferBackedBitmap(final MutableRoaringBitmap mutable)
  {
    mutable.runOptimize();
    final ByteBuffer buffer = ByteBuffer.allocate(mutable.serializedSizeInBytes())
                                        .order(ByteOrder.LITTLE_ENDIAN);
    mutable.serialize(buffer);
    buffer.flip();
    return new ImmutableRoaringBitmap(buffer);
  }

  /**
   * Get the expected result for seeking an iterator to {@code target} and then peeking at the next value.
   */
  private static long expected(final ImmutableRoaringBitmap bitmap, final int target)
  {
    final PeekableIntIterator reference = bitmap.getIntIterator();
    reference.advanceIfNeeded(target);
    return peek(reference);
  }

  /**
   * Peek at the next value, and return it as an unsigned int, or return {@link #NONE} if it is exhausted.
   */
  private static long peek(final PeekableIntIterator iterator)
  {
    return iterator.hasNext() ? Integer.toUnsignedLong(iterator.peekNext()) : NONE;
  }

  /**
   * One past the highest value in the bitmap.
   */
  private static long span(final ImmutableRoaringBitmap bitmap)
  {
    return Integer.toUnsignedLong(bitmap.last()) + 1;
  }
}
