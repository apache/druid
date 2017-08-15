/* 
 * (c) 2010 Alessandro Colantonio
 * <mailto:colanton@mat.uniroma3.it>
 * <http://ricerca.mat.uniroma3.it/users/colanton>
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package io.druid.extendedset.intset;


import io.druid.java.util.common.StringUtils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Formatter;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.SortedSet;

/**
 * This is CONCISE: COmpressed 'N' Composable Integer SEt.
 * <p/>
 * This class is an instance of {@link IntSet} internally represented by
 * compressed bitmaps though a RLE (Run-Length Encoding) compression algorithm.
 * See <a
 * href="http://ricerca.mat.uniroma3.it/users/dipietro/publications/0020-0190.pdf">
 * http://ricerca.mat.uniroma3.it/users/dipietro/publications/0020-0190.pdf</a>
 * for more details.
 * <p/>
 * Notice that the iterator by {@link #iterator()} is <i>fail-fast</i>,
 * similar to most {@link Collection}-derived classes. If the set is
 * structurally modified at any time after the iterator is created, the iterator
 * will throw a {@link ConcurrentModificationException}. Thus, in the face of
 * concurrent modification, the iterator fails quickly and cleanly, rather than
 * risking arbitrary, non-deterministic behavior at an undetermined time in the
 * future. The iterator throws a {@link ConcurrentModificationException} on a
 * best-effort basis. Therefore, it would be wrong to write a program that
 * depended on this exception for its correctness: <i>the fail-fast behavior of
 * iterators should be used only to detect bugs.</i>
 *
 * @author Alessandro Colantonio
 * @version $Id$
 */
public class ConciseSet extends AbstractIntSet implements Serializable
{
  /**
   * generated serial ID
   */
  private static final long serialVersionUID = 560068054685367266L;
  /**
   * <code>true</code> if the class must simulate the behavior of WAH
   */
  private final boolean simulateWAH;
  /**
   * This is the compressed bitmap, that is a collection of words. For each
   * word:
   * <ul>
   * <li> <tt>1* (0x80000000)</tt> means that it is a 31-bit <i>literal</i>.
   * <li> <tt>00* (0x00000000)</tt> indicates a <i>sequence</i> made up of at
   * most one set bit in the first 31 bits, and followed by blocks of 31 0's.
   * The following 5 bits (<tt>00xxxxx*</tt>) indicates which is the set bit (
   * <tt>00000</tt> = no set bit, <tt>00001</tt> = LSB, <tt>11111</tt> = MSB),
   * while the remaining 25 bits indicate the number of following 0's blocks.
   * <li> <tt>01* (0x40000000)</tt> indicates a <i>sequence</i> made up of at
   * most one <i>un</i>set bit in the first 31 bits, and followed by blocks of
   * 31 1's. (see the <tt>00*</tt> case above).
   * </ul>
   * <p/>
   * Note that literal words 0xFFFFFFFF and 0x80000000 are allowed, thus
   * zero-length sequences (i.e., such that getSequenceCount() == 0) cannot
   * exists.
   */
  private int[] words;
  /**
   * Most significant set bit within the uncompressed bit string.
   */
  private transient int last;
  /**
   * Cached cardinality of the bit-set. Defined for efficient {@link #size()}
   * calls. When -1, the cache is invalid.
   */
  private transient int size;
  /**
   * Index of the last word in {@link #words}
   */
  private transient int lastWordIndex;

  /**
   * Creates an empty integer set
   */
  public ConciseSet()
  {
    this(false);
  }

  /**
   * Creates an empty integer set
   *
   * @param simulateWAH <code>true</code> if the class must simulate the behavior of
   *                    WAH
   */
  public ConciseSet(boolean simulateWAH)
  {
    this.simulateWAH = simulateWAH;
    reset();
  }

  public ConciseSet(int[] words, boolean simulateWAH)
  {
    this.words = words;
    this.lastWordIndex = isEmpty() ? -1 : words.length - 1;
    this.size = -1;
    updateLast();
    this.simulateWAH = simulateWAH;
  }

  /**
   * Calculates the modulus division by 31 in a faster way than using <code>n % 31</code>
   * <p/>
   * This method of finding modulus division by an integer that is one less
   * than a power of 2 takes at most <tt>O(lg(32))</tt> time. The number of operations
   * is at most <tt>12 + 9 * ceil(lg(32))</tt>.
   * <p/>
   * See <a href="http://graphics.stanford.edu/~seander/bithacks.html">
   *   http://graphics.stanford.edu/~seander/bithacks.html</a>
   *
   * @param n number to divide
   *
   * @return <code>n % 31</code>
   */
  private static int maxLiteralLengthModulus(int n)
  {
    int m = (n & 0xC1F07C1F) + ((n >>> 5) & 0xC1F07C1F);
    m = (m >>> 15) + (m & 0x00007FFF);
    if (m <= 31) {
      return m == 31 ? 0 : m;
    }
    m = (m >>> 5) + (m & 0x0000001F);
    if (m <= 31) {
      return m == 31 ? 0 : m;
    }
    m = (m >>> 5) + (m & 0x0000001F);
    if (m <= 31) {
      return m == 31 ? 0 : m;
    }
    m = (m >>> 5) + (m & 0x0000001F);
    if (m <= 31) {
      return m == 31 ? 0 : m;
    }
    m = (m >>> 5) + (m & 0x0000001F);
    if (m <= 31) {
      return m == 31 ? 0 : m;
    }
    m = (m >>> 5) + (m & 0x0000001F);
    return m == 31 ? 0 : m;
  }

  /**
   * Calculates the multiplication by 31 in a faster way than using <code>n * 31</code>
   *
   * @param n number to multiply
   *
   * @return <code>n * 31</code>
   */
  private static int maxLiteralLengthMultiplication(int n)
  {
    return (n << 5) - n;
  }

  /**
   * Calculates the division by 31
   *
   * @param n number to divide
   *
   * @return <code>n / 31</code>
   */
  private static int maxLiteralLengthDivision(int n)
  {
    return n / 31;
  }

  /**
   * Checks whether a word is a literal one
   *
   * @param word word to check
   *
   * @return <code>true</code> if the given word is a literal word
   */
  private static boolean isLiteral(int word)
  {
    // "word" must be 1*
    // NOTE: this is faster than "return (word & 0x80000000) == 0x80000000"
    return (word & 0x80000000) != 0;
  }

  /**
   * Checks whether a word contains a sequence of 1's
   *
   * @param word word to check
   *
   * @return <code>true</code> if the given word is a sequence of 1's
   */
  private static boolean isOneSequence(int word)
  {
    // "word" must be 01*
    return (word & 0xC0000000) == ConciseSetUtils.SEQUENCE_BIT;
  }

  /**
   * Checks whether a word contains a sequence of 0's
   *
   * @param word word to check
   *
   * @return <code>true</code> if the given word is a sequence of 0's
   */
  private static boolean isZeroSequence(int word)
  {
    // "word" must be 00*
    return (word & 0xC0000000) == 0;
  }

  /**
   * Checks whether a word contains a sequence of 0's with no set bit, or 1's
   * with no unset bit.
   * <p/>
   * <b>NOTE:</b> when {@link #simulateWAH} is <code>true</code>, it is
   * equivalent to (and as fast as) <code>!</code>{@link #isLiteral(int)}
   *
   * @param word word to check
   *
   * @return <code>true</code> if the given word is a sequence of 0's or 1's
   * but with no (un)set bit
   */
  private static boolean isSequenceWithNoBits(int word)
  {
    // "word" must be 0?00000*
    return (word & 0xBE000000) == 0x00000000;
  }

  /**
   * Gets the number of blocks of 1's or 0's stored in a sequence word
   *
   * @param word word to check
   *
   * @return the number of blocks that follow the first block of 31 bits
   */
  private static int getSequenceCount(int word)
  {
    // get the 25 LSB bits
    return word & 0x01FFFFFF;
  }

  /**
   * Clears the (un)set bit in a sequence
   *
   * @param word word to check
   *
   * @return the sequence corresponding to the given sequence and with no
   * (un)set bits
   */
  private static int getSequenceWithNoBits(int word)
  {
    // clear 29 to 25 LSB bits
    return (word & 0xC1FFFFFF);
  }

  /**
   * Gets the position of the flipped bit within a sequence word. If the
   * sequence has no set/unset bit, returns -1.
   * <p/>
   * Note that the parameter <i>must</i> a sequence word, otherwise the
   * result is meaningless.
   *
   * @param word sequence word to check
   *
   * @return the position of the set bit, from 0 to 31. If the sequence has no
   * set/unset bit, returns -1.
   */
  private static int getFlippedBit(int word)
  {
    // get bits from 30 to 26
    // NOTE: "-1" is required since 00000 represents no bits and 00001 the LSB bit set
    return ((word >>> 25) & 0x0000001F) - 1;
  }

  /**
   * Gets the number of set bits within the literal word
   *
   * @param word literal word
   *
   * @return the number of set bits within the literal word
   */
  private static int getLiteralBitCount(int word)
  {
    return Integer.bitCount(getLiteralBits(word));
  }

  /**
   * Gets the bits contained within the literal word
   *
   * @param word literal word
   *
   * @return the literal word with the most significant bit cleared
   */
  private static int getLiteralBits(int word)
  {
    return ConciseSetUtils.ALL_ONES_WITHOUT_MSB & word;
  }

  /**
   * Returns <code>true</code> when the given 31-bit literal string (namely,
   * with MSB set) contains only one set bit
   *
   * @param literal literal word (namely, with MSB unset)
   *
   * @return <code>true</code> when the given literal contains only one set
   * bit
   */
  private static boolean containsOnlyOneBit(int literal)
  {
    return (literal & (literal - 1)) == 0;
  }

  /**
   * Generates the 32-bit binary representation of a given word (debug only)
   *
   * @param word word to represent
   *
   * @return 32-character string that represents the given word
   */
  private static String toBinaryString(int word)
  {
    String lsb = Integer.toBinaryString(word);
    StringBuilder pad = new StringBuilder();
    for (int i = lsb.length(); i < 32; i++) {
      pad.append('0');
    }
    return pad.append(lsb).toString();
  }

  /**
   * Resets to an empty set
   *
   * @see #ConciseSet()
   * {@link #clear()}
   */
  private void reset()
  {
    words = null;
    last = -1;
    size = 0;
    lastWordIndex = -1;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ConciseSet clone()
  {
    if (isEmpty()) {
      return empty();
    }

    // NOTE: do not use super.clone() since it is 10 times slower!
    ConciseSet res = empty();
    res.last = last;
    res.lastWordIndex = lastWordIndex;
    res.size = size;
    res.words = Arrays.copyOf(words, lastWordIndex + 1);
    return res;
  }

  /**
   * Gets the literal word that represents the first 31 bits of the given the
   * word (i.e. the first block of a sequence word, or the bits of a literal word).
   * <p/>
   * If the word is a literal, it returns the unmodified word. In case of a
   * sequence, it returns a literal that represents the first 31 bits of the
   * given sequence word.
   *
   * @param word word to check
   *
   * @return the literal contained within the given word, <i>with the most
   * significant bit set to 1</i>.
   */
  private /*static*/ int getLiteral(int word)
  {
    if (isLiteral(word)) {
      return word;
    }

    if (simulateWAH) {
      return isZeroSequence(word) ? ConciseSetUtils.ALL_ZEROS_LITERAL : ConciseSetUtils.ALL_ONES_LITERAL;
    }

    // get bits from 30 to 26 and use them to set the corresponding bit
    // NOTE: "1 << (word >>> 25)" and "1 << ((word >>> 25) & 0x0000001F)" are equivalent
    // NOTE: ">>> 1" is required since 00000 represents no bits and 00001 the LSB bit set
    int literal = (1 << (word >>> 25)) >>> 1;
    return isZeroSequence(word)
           ? (ConciseSetUtils.ALL_ZEROS_LITERAL | literal)
           : (ConciseSetUtils.ALL_ONES_LITERAL & ~literal);
  }

  /**
   * Clears bits from MSB (excluded, since it indicates the word type) to the
   * specified bit (excluded). Last word is supposed to be a literal one.
   *
   * @param lastSetBit leftmost bit to preserve
   */
  private void clearBitsAfterInLastWord(int lastSetBit)
  {
    words[lastWordIndex] &= ConciseSetUtils.ALL_ZEROS_LITERAL | (0xFFFFFFFF >>> (31 - lastSetBit));
  }

  /**
   * Assures that the length of {@link #words} is sufficient to contain
   * the given index.
   */
  private void ensureCapacity(int index)
  {
    int capacity = words == null ? 0 : words.length;
    if (capacity > index) {
      return;
    }
    capacity = Math.max(capacity << 1, index + 1);

    if (words == null) {
      // nothing to copy
      words = new int[capacity];
      return;
    }
    words = Arrays.copyOf(words, capacity);
  }

  /**
   * Removes unused allocated words at the end of {@link #words} only when they
   * are more than twice of the needed space
   */
  private void compact()
  {
    if (words != null && ((lastWordIndex + 1) << 1) < words.length) {
      words = Arrays.copyOf(words, lastWordIndex + 1);
    }
  }

  /**
   * Sets the bit at the given absolute position within the uncompressed bit
   * string. The bit <i>must</i> be appendable, that is it must represent an
   * integer that is strictly greater than the maximum integer in the set.
   * Note that the parameter range check is performed by the public method
   * {@link #add)} and <i>not</i> in this method.
   * <p/>
   * <b>NOTE:</b> This method assumes that the last element of {@link #words}
   * (i.e. <code>getLastWord()</code>) <i>must</i> be one of the
   * following:
   * <ul>
   * <li> a literal word with <i>at least one</i> set bit;
   * <li> a sequence of ones.
   * </ul>
   * Hence, the last word in {@link #words} <i>cannot</i> be:
   * <ul>
   * <li> a literal word containing only zeros;
   * <li> a sequence of zeros.
   * </ul>
   *
   * @param i the absolute position of the bit to set (i.e., the integer to add)
   */
  private void append(int i)
  {
    // special case of empty set
    if (isEmpty()) {
      int zeroBlocks = maxLiteralLengthDivision(i);
      if (zeroBlocks == 0) {
        words = new int[1];
        lastWordIndex = 0;
      } else if (zeroBlocks == 1) {
        words = new int[2];
        lastWordIndex = 1;
        words[0] = ConciseSetUtils.ALL_ZEROS_LITERAL;
      } else {
        words = new int[2];
        lastWordIndex = 1;
        words[0] = zeroBlocks - 1;
      }
      last = i;
      size = 1;
      words[lastWordIndex] = ConciseSetUtils.ALL_ZEROS_LITERAL | (1 << maxLiteralLengthModulus(i));
      return;
    }

    // position of the next bit to set within the current literal
    int bit = maxLiteralLengthModulus(last) + i - last;

    // if we are outside the current literal, add zeros in
    // between the current word and the new 1-bit literal word
    if (bit >= ConciseSetUtils.MAX_LITERAL_LENGTH) {
      int zeroBlocks = maxLiteralLengthDivision(bit) - 1;
      bit = maxLiteralLengthModulus(bit);
      if (zeroBlocks == 0) {
        ensureCapacity(lastWordIndex + 1);
      } else {
        ensureCapacity(lastWordIndex + 2);
        appendFill(zeroBlocks, 0);
      }
      appendLiteral(ConciseSetUtils.ALL_ZEROS_LITERAL | 1 << bit);
    } else {
      words[lastWordIndex] |= 1 << bit;
      if (words[lastWordIndex] == ConciseSetUtils.ALL_ONES_LITERAL) {
        lastWordIndex--;
        appendLiteral(ConciseSetUtils.ALL_ONES_LITERAL);
      }
    }

    // update other info
    last = i;
    if (size >= 0) {
      size++;
    }
  }

  /**
   * Append a literal word after the last word
   *
   * @param word the new literal word. Note that the leftmost bit <b>must</b>
   *             be set to 1.
   */
  private void appendLiteral(int word)
  {
    // when we have a zero sequence of the maximum length (that is,
    // 00.00000.1111111111111111111111111 = 0x01FFFFFF), it could happen
    // that we try to append a zero literal because the result of the given operation must be an
    // empty set. Without the following test, we would have increased the
    // counter of the zero sequence, thus obtaining 0x02000000 that
    // represents a sequence with the first bit set!
    if (lastWordIndex == 0 && word == ConciseSetUtils.ALL_ZEROS_LITERAL && words[0] == 0x01FFFFFF) {
      return;
    }

    // first addition
    if (lastWordIndex < 0) {
      words[lastWordIndex = 0] = word;
      return;
    }

    final int lastWord = words[lastWordIndex];
    if (word == ConciseSetUtils.ALL_ZEROS_LITERAL) {
      if (lastWord == ConciseSetUtils.ALL_ZEROS_LITERAL) {
        words[lastWordIndex] = 1;
      } else if (isZeroSequence(lastWord)) {
        words[lastWordIndex]++;
      } else if (!simulateWAH && containsOnlyOneBit(getLiteralBits(lastWord))) {
        words[lastWordIndex] = 1 | ((1 + Integer.numberOfTrailingZeros(lastWord)) << 25);
      } else {
        words[++lastWordIndex] = word;
      }
    } else if (word == ConciseSetUtils.ALL_ONES_LITERAL) {
      if (lastWord == ConciseSetUtils.ALL_ONES_LITERAL) {
        words[lastWordIndex] = ConciseSetUtils.SEQUENCE_BIT | 1;
      } else if (isOneSequence(lastWord)) {
        words[lastWordIndex]++;
      } else if (!simulateWAH && containsOnlyOneBit(~lastWord)) {
        words[lastWordIndex] = ConciseSetUtils.SEQUENCE_BIT | 1 | ((1 + Integer.numberOfTrailingZeros(~lastWord))
                                                                   << 25);
      } else {
        words[++lastWordIndex] = word;
      }
    } else {
      words[++lastWordIndex] = word;
    }
  }

  /**
   * Append a sequence word after the last word
   *
   * @param length   sequence length
   * @param fillType sequence word with a count that equals 0
   */
  private void appendFill(int length, int fillType)
  {
    assert length > 0;
    assert lastWordIndex >= -1;

    fillType &= ConciseSetUtils.SEQUENCE_BIT;

    // it is actually a literal...
    if (length == 1) {
      appendLiteral(fillType == 0 ? ConciseSetUtils.ALL_ZEROS_LITERAL : ConciseSetUtils.ALL_ONES_LITERAL);
      return;
    }

    // empty set
    if (lastWordIndex < 0) {
      words[lastWordIndex = 0] = fillType | (length - 1);
      return;
    }

    final int lastWord = words[lastWordIndex];
    if (isLiteral(lastWord)) {
      if (fillType == 0 && lastWord == ConciseSetUtils.ALL_ZEROS_LITERAL) {
        words[lastWordIndex] = length;
      } else if (fillType == ConciseSetUtils.SEQUENCE_BIT && lastWord == ConciseSetUtils.ALL_ONES_LITERAL) {
        words[lastWordIndex] = ConciseSetUtils.SEQUENCE_BIT | length;
      } else if (!simulateWAH) {
        if (fillType == 0 && containsOnlyOneBit(getLiteralBits(lastWord))) {
          words[lastWordIndex] = length | ((1 + Integer.numberOfTrailingZeros(lastWord)) << 25);
        } else if (fillType == ConciseSetUtils.SEQUENCE_BIT && containsOnlyOneBit(~lastWord)) {
          words[lastWordIndex] = ConciseSetUtils.SEQUENCE_BIT | length | ((1 + Integer.numberOfTrailingZeros(~lastWord))
                                                                          << 25);
        } else {
          words[++lastWordIndex] = fillType | (length - 1);
        }
      } else {
        words[++lastWordIndex] = fillType | (length - 1);
      }
    } else {
      if ((lastWord & 0xC0000000) == fillType) {
        words[lastWordIndex] += length;
      } else {
        words[++lastWordIndex] = fillType | (length - 1);
      }
    }
  }

  /**
   * Recalculate a fresh value for {@link ConciseSet#last}
   */
  private void updateLast()
  {
    if (isEmpty()) {
      last = -1;
      return;
    }

    last = 0;
    for (int i = 0; i <= lastWordIndex; i++) {
      int w = words[i];
      if (isLiteral(w)) {
        last += ConciseSetUtils.MAX_LITERAL_LENGTH;
      } else {
        last += maxLiteralLengthMultiplication(getSequenceCount(w) + 1);
      }
    }

    int w = words[lastWordIndex];
    if (isLiteral(w)) {
      last -= Integer.numberOfLeadingZeros(getLiteralBits(w));
    } else {
      last--;
    }
  }

  /**
   * Performs the given operation over the bit-sets
   *
   * @param other    {@link ConciseSet} instance that represents the right
   *                 operand
   * @param operator operator
   *
   * @return the result of the operation
   */
  private ConciseSet performOperation(ConciseSet other, Operator operator)
  {
    // non-empty arguments
    if (this.isEmpty() || other.isEmpty()) {
      return operator.combineEmptySets(this, other);
    }

    // if the two operands are disjoint, the operation is faster
    ConciseSet res = operator.combineDisjointSets(this, other);
    if (res != null) {
      return res;
    }

    // Allocate a sufficient number of words to contain all possible results.
    // NOTE: since lastWordIndex is the index of the last used word in "words",
    // we require "+2" to have the actual maximum required space.
    // In any case, we do not allocate more than the maximum space required
    // for the uncompressed representation.
    // Another "+1" is required to allows for the addition of the last word
    // before compacting.
    res = empty();
    res.words = new int[1 + Math.min(
        this.lastWordIndex + other.lastWordIndex + 2,
        maxLiteralLengthDivision(Math.max(this.last, other.last)) << (simulateWAH ? 1 : 0)
    )];

    // scan "this" and "other"
    WordIterator thisItr = new WordIterator();
    WordIterator otherItr = other.new WordIterator();
    while (true) {
      if (!thisItr.isLiteral) {
        if (!otherItr.isLiteral) {
          int minCount = Math.min(thisItr.count, otherItr.count);
          res.appendFill(minCount, operator.combineLiterals(thisItr.word, otherItr.word));
          //noinspection NonShortCircuitBooleanExpression
          if (!thisItr.prepareNext(minCount) | /* NOT || */ !otherItr.prepareNext(minCount)) {
            break;
          }
        } else {
          res.appendLiteral(operator.combineLiterals(thisItr.toLiteral(), otherItr.word));
          thisItr.word--;
          //noinspection NonShortCircuitBooleanExpression
          if (!thisItr.prepareNext(1) | /* do NOT use "||" */ !otherItr.prepareNext()) {
            break;
          }
        }
      } else if (!otherItr.isLiteral) {
        res.appendLiteral(operator.combineLiterals(thisItr.word, otherItr.toLiteral()));
        otherItr.word--;
        //noinspection NonShortCircuitBooleanExpression
        if (!thisItr.prepareNext() | /* do NOT use  "||" */ !otherItr.prepareNext(1)) {
          break;
        }
      } else {
        res.appendLiteral(operator.combineLiterals(thisItr.word, otherItr.word));
        //noinspection NonShortCircuitBooleanExpression
        if (!thisItr.prepareNext() | /* do NOT use  "||" */ !otherItr.prepareNext()) {
          break;
        }
      }
    }

    // invalidate the size
    res.size = -1;
    boolean invalidLast = true;

    // if one bit string is greater than the other one, we add the remaining
    // bits depending on the given operation.
    switch (operator) {
      case AND:
        break;
      case OR:
        res.last = Math.max(this.last, other.last);
        invalidLast = thisItr.flush(res);
        invalidLast |= otherItr.flush(res);
        break;
      case XOR:
        if (this.last != other.last) {
          res.last = Math.max(this.last, other.last);
          invalidLast = false;
        }
        invalidLast |= thisItr.flush(res);
        invalidLast |= otherItr.flush(res);
        break;
      case ANDNOT:
        if (this.last > other.last) {
          res.last = this.last;
          invalidLast = false;
        }
        invalidLast |= thisItr.flush(res);
        break;
    }

    // remove trailing zeros
    res.trimZeros();
    if (res.isEmpty()) {
      return res;
    }

    // compute the greatest element
    if (invalidLast) {
      res.updateLast();
    }

    // compact the memory
    res.compact();

    return res;
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("NonShortCircuitBooleanExpression")
  @Override
  public int intersectionSize(IntSet o)
  {
    // special cases
    if (isEmpty() || o == null || o.isEmpty()) {
      return 0;
    }
    if (this == o) {
      return size();
    }

    final ConciseSet other = convert(o);

    // check whether the first operator starts with a sequence that
    // completely "covers" the second operator
    if (isSequenceWithNoBits(this.words[0])
        && maxLiteralLengthMultiplication(getSequenceCount(this.words[0]) + 1) > other.last) {
      if (isZeroSequence(this.words[0])) {
        return 0;
      }
      return other.size();
    }
    if (isSequenceWithNoBits(other.words[0])
        && maxLiteralLengthMultiplication(getSequenceCount(other.words[0]) + 1) > this.last) {
      if (isZeroSequence(other.words[0])) {
        return 0;
      }
      return this.size();
    }

    int res = 0;

    // scan "this" and "other"
    WordIterator thisItr = new WordIterator();
    WordIterator otherItr = other.new WordIterator();
    while (true) {
      if (!thisItr.isLiteral) {
        if (!otherItr.isLiteral) {
          int minCount = Math.min(thisItr.count, otherItr.count);
          if ((ConciseSetUtils.SEQUENCE_BIT & thisItr.word & otherItr.word) != 0) {
            res += maxLiteralLengthMultiplication(minCount);
          }
          if (!thisItr.prepareNext(minCount) | /* NOT || */ !otherItr.prepareNext(minCount)) {
            break;
          }
        } else {
          res += getLiteralBitCount(thisItr.toLiteral() & otherItr.word);
          thisItr.word--;
          if (!thisItr.prepareNext(1) | /* do NOT use "||" */ !otherItr.prepareNext()) {
            break;
          }
        }
      } else if (!otherItr.isLiteral) {
        res += getLiteralBitCount(thisItr.word & otherItr.toLiteral());
        otherItr.word--;
        if (!thisItr.prepareNext() | /* do NOT use  "||" */ !otherItr.prepareNext(1)) {
          break;
        }
      } else {
        res += getLiteralBitCount(thisItr.word & otherItr.word);
        if (!thisItr.prepareNext() | /* do NOT use  "||" */ !otherItr.prepareNext()) {
          break;
        }
      }
    }

    return res;
  }

  /**
   * {@inheritDoc}
   */
  public ByteBuffer toByteBuffer()
  {
    ByteBuffer buffer = ByteBuffer.allocate((lastWordIndex + 1) * 4);
    buffer.asIntBuffer().put(Arrays.copyOf(words, lastWordIndex + 1));
    return buffer;
  }

  /**
   * {@inheritDoc}
   */
  public int[] getWords()
  {
    if (words == null) {
      return new int[]{};
    }
    return Arrays.copyOf(words, lastWordIndex + 1);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int get(int i)
  {
    if (i < 0) {
      throw new IndexOutOfBoundsException();
    }

    // initialize data
    int firstSetBitInWord = 0;
    int position = i;
    int setBitsInCurrentWord = 0;
    for (int j = 0; j <= lastWordIndex; j++) {
      int w = words[j];
      if (isLiteral(w)) {
        // number of bits in the current word
        setBitsInCurrentWord = getLiteralBitCount(w);

        // check if the desired bit is in the current word
        if (position < setBitsInCurrentWord) {
          int currSetBitInWord = -1;
          for (; position >= 0; position--) {
            currSetBitInWord = Integer.numberOfTrailingZeros(w & (0xFFFFFFFF << (currSetBitInWord + 1)));
          }
          return firstSetBitInWord + currSetBitInWord;
        }

        // skip the 31-bit block
        firstSetBitInWord += ConciseSetUtils.MAX_LITERAL_LENGTH;
      } else {
        // number of involved bits (31 * blocks)
        int sequenceLength = maxLiteralLengthMultiplication(getSequenceCount(w) + 1);

        // check the sequence type
        if (isOneSequence(w)) {
          if (simulateWAH || isSequenceWithNoBits(w)) {
            setBitsInCurrentWord = sequenceLength;
            if (position < setBitsInCurrentWord) {
              return firstSetBitInWord + position;
            }
          } else {
            setBitsInCurrentWord = sequenceLength - 1;
            if (position < setBitsInCurrentWord) {
              // check whether the desired set bit is after the
              // flipped bit (or after the first block)
              return firstSetBitInWord + position + (position < getFlippedBit(w) ? 0 : 1);
            }
          }
        } else {
          if (simulateWAH || isSequenceWithNoBits(w)) {
            setBitsInCurrentWord = 0;
          } else {
            setBitsInCurrentWord = 1;
            if (position == 0) {
              return firstSetBitInWord + getFlippedBit(w);
            }
          }
        }

        // skip the 31-bit blocks
        firstSetBitInWord += sequenceLength;
      }

      // update the number of found set bits
      position -= setBitsInCurrentWord;
    }

    throw new IndexOutOfBoundsException(Integer.toString(i));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int indexOf(int e)
  {
    if (e < 0) {
      throw new IllegalArgumentException("positive integer expected: " + Integer.toString(e));
    }
    if (isEmpty()) {
      return -1;
    }

    // returned value
    int index = 0;

    int blockIndex = maxLiteralLengthDivision(e);
    int bitPosition = maxLiteralLengthModulus(e);
    for (int i = 0; i <= lastWordIndex && blockIndex >= 0; i++) {
      int w = words[i];
      if (isLiteral(w)) {
        // check if the current literal word is the "right" one
        if (blockIndex == 0) {
          if ((w & (1 << bitPosition)) == 0) {
            return -1;
          }
          return index + Integer.bitCount(w & ~(0xFFFFFFFF << bitPosition));
        }
        blockIndex--;
        index += getLiteralBitCount(w);
      } else {
        if (simulateWAH) {
          if (isOneSequence(w) && blockIndex <= getSequenceCount(w)) {
            return index + maxLiteralLengthMultiplication(blockIndex) + bitPosition;
          }
        } else {
          // if we are at the beginning of a sequence, and it is
          // a set bit, the bit already exists
          if (blockIndex == 0) {
            int l = getLiteral(w);
            if ((l & (1 << bitPosition)) == 0) {
              return -1;
            }
            return index + Integer.bitCount(l & ~(0xFFFFFFFF << bitPosition));
          }

          // if we are in the middle of a sequence of 1's, the bit already exist
          if (blockIndex > 0
              && blockIndex <= getSequenceCount(w)
              && isOneSequence(w)) {
            return index + maxLiteralLengthMultiplication(blockIndex) + bitPosition - (isSequenceWithNoBits(w) ? 0 : 1);
          }
        }

        // next word
        int blocks = getSequenceCount(w) + 1;
        blockIndex -= blocks;
        if (isZeroSequence(w)) {
          if (!simulateWAH && !isSequenceWithNoBits(w)) {
            index++;
          }
        } else {
          index += maxLiteralLengthMultiplication(blocks);
          if (!simulateWAH && !isSequenceWithNoBits(w)) {
            index--;
          }
        }
      }
    }

    // not found
    return -1;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ConciseSet intersection(IntSet other)
  {
    if (isEmpty() || other == null || other.isEmpty()) {
      return empty();
    }
    if (other == this) {
      return clone();
    }
    return performOperation(convert(other), Operator.AND);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ConciseSet union(IntSet other)
  {
    if (other == null || other.isEmpty() || other == this) {
      return clone();
    }
    return performOperation(convert(other), Operator.OR);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ConciseSet difference(IntSet other)
  {
    if (other == this) {
      return empty();
    }
    if (other == null || other.isEmpty()) {
      return clone();
    }
    return performOperation(convert(other), Operator.ANDNOT);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ConciseSet symmetricDifference(IntSet other)
  {
    if (other == this) {
      return empty();
    }
    if (other == null || other.isEmpty()) {
      return clone();
    }
    return performOperation(convert(other), Operator.XOR);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ConciseSet complemented()
  {
    ConciseSet cloned = clone();
    cloned.complement();
    return cloned;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void complement()
  {

    if (isEmpty()) {
      return;
    }

    if (last == ConciseSetUtils.MIN_ALLOWED_SET_BIT) {
      clear();
      return;
    }

    // update size
    if (size >= 0) {
      size = last - size + 1;
    }

    // complement each word
    for (int i = 0; i <= lastWordIndex; i++) {
      int w = words[i];
      if (isLiteral(w)) {
        // negate the bits and set the most significant bit to 1
        words[i] = ConciseSetUtils.ALL_ZEROS_LITERAL | ~w;
      } else {
        // switch the sequence type
        words[i] ^= ConciseSetUtils.SEQUENCE_BIT;
      }
    }

    // do not complement after the last element
    if (isLiteral(words[lastWordIndex])) {
      clearBitsAfterInLastWord(maxLiteralLengthModulus(last));
    }

    // remove trailing zeros
    trimZeros();
    if (isEmpty()) {
      return;
    }

    // calculate the maximal element
    last = 0;
    int w = 0;
    for (int i = 0; i <= lastWordIndex; i++) {
      w = words[i];
      if (isLiteral(w)) {
        last += ConciseSetUtils.MAX_LITERAL_LENGTH;
      } else {
        last += maxLiteralLengthMultiplication(getSequenceCount(w) + 1);
      }
    }

    // manage the last word (that must be a literal or a sequence of 1's)
    if (isLiteral(w)) {
      last -= Integer.numberOfLeadingZeros(getLiteralBits(w));
    } else {
      last--;
    }
  }

  /**
   * Removes trailing zeros
   */
  private void trimZeros()
  {
    // loop over ALL_ZEROS_LITERAL words
    int w;
    do {
      w = words[lastWordIndex];
      if (w == ConciseSetUtils.ALL_ZEROS_LITERAL) {
        lastWordIndex--;
      } else if (isZeroSequence(w)) {
        if (simulateWAH || isSequenceWithNoBits(w)) {
          lastWordIndex--;
        } else {
          // convert the sequence in a 1-bit literal word
          words[lastWordIndex] = getLiteral(w);
          return;
        }
      } else {
        // one sequence or literal
        return;
      }
      if (lastWordIndex < 0) {
        reset();
        return;
      }
    } while (true);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IntIterator iterator()
  {
    if (isEmpty()) {
      return EmptyIntIterator.instance();
    }
    return new BitIterator();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IntIterator descendingIterator()
  {
    if (isEmpty()) {
      return EmptyIntIterator.instance();
    }
    return new ReverseBitIterator();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void clear()
  {
    reset();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int last()
  {
    if (isEmpty()) {
      throw new NoSuchElementException();
    }
    return last;
  }

  /**
   * Convert a given collection to a {@link ConciseSet} instance
   */
  private ConciseSet convert(IntSet c)
  {
    if (c instanceof ConciseSet && simulateWAH == ((ConciseSet) c).simulateWAH) {
      return (ConciseSet) c;
    }
    if (c == null) {
      return empty();
    }

    ConciseSet res = empty();
    IntIterator itr = c.iterator();
    while (itr.hasNext()) {
      res.add(itr.next());
    }
    return res;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ConciseSet convert(int... a)
  {
    ConciseSet res = empty();
    if (a != null) {
      a = Arrays.copyOf(a, a.length);
      Arrays.sort(a);
      for (int i : a) {
        if (res.last != i) {
          res.add(i);
        }
      }
    }
    return res;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ConciseSet convert(Collection<Integer> c)
  {
    ConciseSet res = empty();
    Collection<Integer> sorted;
    if (c != null) {
      if (c instanceof SortedSet<?> && ((SortedSet<?>) c).comparator() == null) {
        sorted = c;
      } else {
        sorted = new ArrayList<Integer>(c);
        Collections.sort((List<Integer>) sorted);
      }
      for (int i : sorted) {
        if (res.last != i) {
          res.add(i);
        }
      }
    }
    return res;
  }

  /**
   * Replace the current instance with another {@link ConciseSet} instance. It
   * also returns <code>true</code> if the given set is actually different
   * from the current one
   *
   * @param other {@link ConciseSet} instance to use to replace the current one
   *
   * @return <code>true</code> if the given set is different from the current
   * set
   */
  private boolean replaceWith(ConciseSet other)
  {
    if (this == other) {
      return false;
    }

    boolean isSimilar = (this.lastWordIndex == other.lastWordIndex)
                        && (this.last == other.last);
    for (int i = 0; isSimilar && (i <= lastWordIndex); i++) {
      isSimilar = this.words[i] == other.words[i];
    }

    if (isSimilar) {
      if (other.size >= 0) {
        this.size = other.size;
      }
      return false;
    }

    this.words = other.words;
    this.size = other.size;
    this.last = other.last;
    this.lastWordIndex = other.lastWordIndex;
    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean add(int e)
  {

    // range check
    if (e < ConciseSetUtils.MIN_ALLOWED_SET_BIT || e > ConciseSetUtils.MAX_ALLOWED_INTEGER) {
      throw new IndexOutOfBoundsException(String.valueOf(e));
    }

    // the element can be simply appended
    if (e > last) {
      append(e);
      return true;
    }

    if (e == last) {
      return false;
    }

    // check if the element can be put in a literal word
    int blockIndex = maxLiteralLengthDivision(e);
    int bitPosition = maxLiteralLengthModulus(e);
    for (int i = 0; i <= lastWordIndex && blockIndex >= 0; i++) {
      int w = words[i];
      if (isLiteral(w)) {
        // check if the current literal word is the "right" one
        if (blockIndex == 0) {
          // bit already set
          if ((w & (1 << bitPosition)) != 0) {
            return false;
          }

          // By adding the bit we potentially create a sequence:
          // -- If the literal is made up of all zeros, it definitely
          //    cannot be part of a sequence (otherwise it would not have
          //    been created). Thus, we can create a 1-bit literal word
          // -- If there are MAX_LITERAL_LENGTH - 2 set bits, by adding
          //    the new one we potentially allow for a 1's sequence
          //    together with the successive word
          // -- If there are MAX_LITERAL_LENGTH - 1 set bits, by adding
          //    the new one we potentially allow for a 1's sequence
          //    together with the successive and/or the preceding words
          if (!simulateWAH) {
            int bitCount = getLiteralBitCount(w);
            if (bitCount >= ConciseSetUtils.MAX_LITERAL_LENGTH - 2) {
              break;
            }
          } else {
            if (containsOnlyOneBit(~w) || w == ConciseSetUtils.ALL_ONES_LITERAL) {
              break;
            }
          }

          // set the bit
          words[i] |= 1 << bitPosition;
          if (size >= 0) {
            size++;
          }
          return true;
        }

        blockIndex--;
      } else {
        if (simulateWAH) {
          if (isOneSequence(w) && blockIndex <= getSequenceCount(w)) {
            return false;
          }
        } else {
          // if we are at the beginning of a sequence, and it is
          // a set bit, the bit already exists
          if (blockIndex == 0
              && (getLiteral(w) & (1 << bitPosition)) != 0) {
            return false;
          }

          // if we are in the middle of a sequence of 1's, the bit already exist
          if (blockIndex > 0
              && blockIndex <= getSequenceCount(w)
              && isOneSequence(w)) {
            return false;
          }
        }

        // next word
        blockIndex -= getSequenceCount(w) + 1;
      }
    }

    // the bit is in the middle of a sequence or it may cause a literal to
    // become a sequence, thus the "easiest" way to add it is by ORing
    return replaceWith(performOperation(convert(e), Operator.OR));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean remove(int o)
  {

    if (isEmpty()) {
      return false;
    }

    // the element cannot exist
    if (o > last) {
      return false;
    }

    // check if the element can be removed from a literal word
    int blockIndex = maxLiteralLengthDivision(o);
    int bitPosition = maxLiteralLengthModulus(o);
    for (int i = 0; i <= lastWordIndex && blockIndex >= 0; i++) {
      final int w = words[i];
      if (isLiteral(w)) {
        // check if the current literal word is the "right" one
        if (blockIndex == 0) {
          // the bit is already unset
          if ((w & (1 << bitPosition)) == 0) {
            return false;
          }

          // By removing the bit we potentially create a sequence:
          // -- If the literal is made up of all ones, it definitely
          //    cannot be part of a sequence (otherwise it would not have
          //    been created). Thus, we can create a 30-bit literal word
          // -- If there are 2 set bits, by removing the specified
          //    one we potentially allow for a 1's sequence together with
          //    the successive word
          // -- If there is 1 set bit, by removing the new one we
          //    potentially allow for a 0's sequence
          //    together with the successive and/or the preceding words
          if (!simulateWAH) {
            int bitCount = getLiteralBitCount(w);
            if (bitCount <= 2) {
              break;
            }
          } else {
            final int l = getLiteralBits(w);
            if (l == 0 || containsOnlyOneBit(l)) {
              break;
            }
          }

          // unset the bit
          words[i] &= ~(1 << bitPosition);
          if (size >= 0) {
            size--;
          }

          // if the bit is the maximal element, update it
          if (o == last) {
            last -= maxLiteralLengthModulus(last) - (ConciseSetUtils.MAX_LITERAL_LENGTH
                                                     - Integer.numberOfLeadingZeros(getLiteralBits(words[i])));
          }
          return true;
        }

        blockIndex--;
      } else {
        if (simulateWAH) {
          if (isZeroSequence(w) && blockIndex <= getSequenceCount(w)) {
            return false;
          }
        } else {
          // if we are at the beginning of a sequence, and it is
          // an unset bit, the bit does not exist
          if (blockIndex == 0
              && (getLiteral(w) & (1 << bitPosition)) == 0) {
            return false;
          }

          // if we are in the middle of a sequence of 0's, the bit does not exist
          if (blockIndex > 0
              && blockIndex <= getSequenceCount(w)
              && isZeroSequence(w)) {
            return false;
          }
        }

        // next word
        blockIndex -= getSequenceCount(w) + 1;
      }
    }

    // the bit is in the middle of a sequence or it may cause a literal to
    // become a sequence, thus the "easiest" way to remove it by ANDNOTing
    return replaceWith(performOperation(convert(o), Operator.ANDNOT));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean contains(int o)
  {
    if (isEmpty() || o > last || o < 0) {
      return false;
    }

    // check if the element is within a literal word
    int block = maxLiteralLengthDivision(o);
    int bit = maxLiteralLengthModulus(o);
    for (int i = 0; i <= lastWordIndex; i++) {
      final int w = words[i];
      final int t = w & 0xC0000000; // the first two bits...
      switch (t) {
        case 0x80000000:  // LITERAL
        case 0xC0000000:  // LITERAL
          // check if the current literal word is the "right" one
          if (block == 0) {
            return (w & (1 << bit)) != 0;
          }
          block--;
          break;
        case 0x00000000:  // ZERO SEQUENCE
          if (!simulateWAH) {
            if (block == 0 && ((w >> 25) - 1) == bit) {
              return true;
            }
          }
          block -= getSequenceCount(w) + 1;
          if (block < 0) {
            return false;
          }
          break;
        case 0x40000000:  // ONE SEQUENCE
          if (!simulateWAH) {
            if (block == 0 && (0x0000001F & (w >> 25) - 1) == bit) {
              return false;
            }
          }
          block -= getSequenceCount(w) + 1;
          if (block < 0) {
            return true;
          }
          break;
      }
    }

    // no more words
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean containsAll(IntSet c)
  {
    if (c == null || c.isEmpty() || c == this) {
      return true;
    }
    if (isEmpty()) {
      return false;
    }

    final ConciseSet other = convert(c);
    if (other.last > last) {
      return false;
    }
    if (size >= 0 && other.size > size) {
      return false;
    }
    if (other.size == 1) {
      return contains(other.last);
    }

    // check whether the first operator starts with a sequence that
    // completely "covers" the second operator
    if (isSequenceWithNoBits(this.words[0])
        && maxLiteralLengthMultiplication(getSequenceCount(this.words[0]) + 1) > other.last) {
      return !isZeroSequence(this.words[0]);
    }
    if (isSequenceWithNoBits(other.words[0])
        && maxLiteralLengthMultiplication(getSequenceCount(other.words[0]) + 1) > this.last) {
      return false;
    }

    // scan "this" and "other"
    WordIterator thisItr = new WordIterator();
    WordIterator otherItr = other.new WordIterator();
    while (true) {
      if (!thisItr.isLiteral) {
        if (!otherItr.isLiteral) {
          int minCount = Math.min(thisItr.count, otherItr.count);
          if ((ConciseSetUtils.SEQUENCE_BIT & thisItr.word) == 0
              && (ConciseSetUtils.SEQUENCE_BIT & otherItr.word) != 0) {
            return false;
          }
          if (!otherItr.prepareNext(minCount)) {
            return true;
          }
          if (!thisItr.prepareNext(minCount)) {
            return false;
          }
        } else {
          if ((thisItr.toLiteral() & otherItr.word) != otherItr.word) {
            return false;
          }
          thisItr.word--;
          if (!otherItr.prepareNext()) {
            return true;
          }
          if (!thisItr.prepareNext(1)) {
            return false;
          }
        }
      } else if (!otherItr.isLiteral) {
        int o = otherItr.toLiteral();
        if ((thisItr.word & otherItr.toLiteral()) != o) {
          return false;
        }
        otherItr.word--;
        if (!otherItr.prepareNext(1)) {
          return true;
        }
        if (!thisItr.prepareNext()) {
          return false;
        }
      } else {
        if ((thisItr.word & otherItr.word) != otherItr.word) {
          return false;
        }
        if (!otherItr.prepareNext()) {
          return true;
        }
        if (!thisItr.prepareNext()) {
          return false;
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean containsAny(IntSet c)
  {
    if (c == null || c.isEmpty() || c == this) {
      return true;
    }
    if (isEmpty()) {
      return false;
    }

    final ConciseSet other = convert(c);
    if (other.size == 1) {
      return contains(other.last);
    }

    // disjoint sets
    if (isSequenceWithNoBits(this.words[0])
        && maxLiteralLengthMultiplication(getSequenceCount(this.words[0]) + 1) > other.last) {
      return !isZeroSequence(this.words[0]);
    }
    if (isSequenceWithNoBits(other.words[0])
        && maxLiteralLengthMultiplication(getSequenceCount(other.words[0]) + 1) > this.last) {
      return !isZeroSequence(other.words[0]);
    }

    // scan "this" and "other"
    WordIterator thisItr = new WordIterator();
    WordIterator otherItr = other.new WordIterator();
    while (true) {
      if (!thisItr.isLiteral) {
        if (!otherItr.isLiteral) {
          int minCount = Math.min(thisItr.count, otherItr.count);
          if ((ConciseSetUtils.SEQUENCE_BIT & thisItr.word & otherItr.word) != 0) {
            return true;
          }
          //noinspection NonShortCircuitBooleanExpression
          if (!thisItr.prepareNext(minCount) |  /* NOT || */ !otherItr.prepareNext(minCount)) {
            return false;
          }
        } else {
          if ((thisItr.toLiteral() & otherItr.word) != ConciseSetUtils.ALL_ZEROS_LITERAL) {
            return true;
          }
          thisItr.word--;
          //noinspection NonShortCircuitBooleanExpression
          if (!thisItr.prepareNext(1) | /* do NOT use "||" */ !otherItr.prepareNext()) {
            return false;
          }
        }
      } else if (!otherItr.isLiteral) {
        if ((thisItr.word & otherItr.toLiteral()) != ConciseSetUtils.ALL_ZEROS_LITERAL) {
          return true;
        }
        otherItr.word--;
        //noinspection NonShortCircuitBooleanExpression
        if (!thisItr.prepareNext() | /* do NOT use  "||" */ !otherItr.prepareNext(1)) {
          return false;
        }
      } else {
        if ((thisItr.word & otherItr.word) != ConciseSetUtils.ALL_ZEROS_LITERAL) {
          return true;
        }
        //noinspection NonShortCircuitBooleanExpression
        if (!thisItr.prepareNext() | /* do NOT use  "||" */ !otherItr.prepareNext()) {
          return false;
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean containsAtLeast(IntSet c, int minElements)
  {
    if (minElements < 1) {
      throw new IllegalArgumentException();
    }
    if ((size >= 0 && size < minElements) || c == null || c.isEmpty() || isEmpty()) {
      return false;
    }
    if (this == c) {
      return size() >= minElements;
    }

    // convert the other set in order to perform a more complex intersection
    ConciseSet other = convert(c);
    if (other.size >= 0 && other.size < minElements) {
      return false;
    }
    if (minElements == 1 && other.size == 1) {
      return contains(other.last);
    }
    if (minElements == 1 && size == 1) {
      return other.contains(last);
    }

    // disjoint sets
    if (isSequenceWithNoBits(this.words[0])
        && maxLiteralLengthMultiplication(getSequenceCount(this.words[0]) + 1) > other.last) {
      return !isZeroSequence(this.words[0]);
    }
    if (isSequenceWithNoBits(other.words[0])
        && maxLiteralLengthMultiplication(getSequenceCount(other.words[0]) + 1) > this.last) {
      return !isZeroSequence(other.words[0]);
    }

    // resulting size
    int res = 0;

    // scan "this" and "other"
    WordIterator thisItr = new WordIterator();
    WordIterator otherItr = other.new WordIterator();
    while (true) {
      if (!thisItr.isLiteral) {
        if (!otherItr.isLiteral) {
          int minCount = Math.min(thisItr.count, otherItr.count);
          if ((ConciseSetUtils.SEQUENCE_BIT & thisItr.word & otherItr.word) != 0) {
            res += maxLiteralLengthMultiplication(minCount);
            if (res >= minElements) {
              return true;
            }
          }
          //noinspection NonShortCircuitBooleanExpression
          if (!thisItr.prepareNext(minCount) |  /* NOT || */ !otherItr.prepareNext(minCount)) {
            return false;
          }
        } else {
          res += getLiteralBitCount(thisItr.toLiteral() & otherItr.word);
          if (res >= minElements) {
            return true;
          }
          thisItr.word--;
          //noinspection NonShortCircuitBooleanExpression
          if (!thisItr.prepareNext(1) | /* do NOT use "||" */ !otherItr.prepareNext()) {
            return false;
          }
        }
      } else if (!otherItr.isLiteral) {
        res += getLiteralBitCount(thisItr.word & otherItr.toLiteral());
        if (res >= minElements) {
          return true;
        }
        otherItr.word--;
        //noinspection NonShortCircuitBooleanExpression
        if (!thisItr.prepareNext() | /* do NOT use  "||" */ !otherItr.prepareNext(1)) {
          return false;
        }
      } else {
        res += getLiteralBitCount(thisItr.word & otherItr.word);
        if (res >= minElements) {
          return true;
        }
        //noinspection NonShortCircuitBooleanExpression
        if (!thisItr.prepareNext() | /* do NOT use  "||" */ !otherItr.prepareNext()) {
          return false;
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isEmpty()
  {
    return words == null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean retainAll(IntSet c)
  {

    if (isEmpty() || c == this) {
      return false;
    }
    if (c == null || c.isEmpty()) {
      clear();
      return true;
    }

    ConciseSet other = convert(c);
    if (other.size == 1) {
      if (contains(other.last)) {
        if (size == 1) {
          return false;
        }
        return replaceWith(convert(other.last));
      }
      clear();
      return true;
    }

    return replaceWith(performOperation(other, Operator.AND));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean addAll(IntSet c)
  {
    if (c == null || c.isEmpty() || this == c) {
      return false;
    }

    ConciseSet other = convert(c);
    if (other.size == 1) {
      return add(other.last);
    }

    return replaceWith(performOperation(convert(c), Operator.OR));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean removeAll(IntSet c)
  {

    if (c == null || c.isEmpty() || isEmpty()) {
      return false;
    }
    if (c == this) {
      clear();
      return true;
    }

    ConciseSet other = convert(c);
    if (other.size == 1) {
      return remove(other.last);
    }

    return replaceWith(performOperation(convert(c), Operator.ANDNOT));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int size()
  {
    if (size < 0) {
      size = 0;
      for (int i = 0; i <= lastWordIndex; i++) {
        int w = words[i];
        if (isLiteral(w)) {
          size += getLiteralBitCount(w);
        } else {
          if (isZeroSequence(w)) {
            if (!isSequenceWithNoBits(w)) {
              size++;
            }
          } else {
            size += maxLiteralLengthMultiplication(getSequenceCount(w) + 1);
            if (!isSequenceWithNoBits(w)) {
              size--;
            }
          }
        }
      }
    }
    return size;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ConciseSet empty()
  {
    return new ConciseSet(simulateWAH);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode()
  {
    int h = 1;
    for (int i = 0; i <= lastWordIndex; i++) {
      h = (h << 5) - h + words[i];
    }
    return h;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(Object obj)
  {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof ConciseSet)) {
      return super.equals(obj);
    }

    final ConciseSet other = (ConciseSet) obj;
    if (simulateWAH != other.simulateWAH) {
      return super.equals(obj);
    }

    if (size() != other.size()) {
      return false;
    }
    if (isEmpty()) {
      return true;
    }
    if (last != other.last) {
      return false;
    }
    for (int i = 0; i <= lastWordIndex; i++) {
      if (words[i] != other.words[i]) {
        return false;
      }
    }
    return true;
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("CompareToUsesNonFinalVariable")
  @Override
  public int compareTo(IntSet o)
  {
    // empty set cases
    if (this.isEmpty() && o.isEmpty()) {
      return 0;
    }
    if (this.isEmpty()) {
      return -1;
    }
    if (o.isEmpty()) {
      return 1;
    }

    final ConciseSet other = convert(o);

    // the word at the end must be the same
    int res = this.last - other.last;
    if (res != 0) {
      return res < 0 ? -1 : 1;
    }

    // scan words from MSB to LSB
    int thisIndex = this.lastWordIndex;
    int otherIndex = other.lastWordIndex;
    int thisWord = this.words[thisIndex];
    int otherWord = other.words[otherIndex];
    while (thisIndex >= 0 && otherIndex >= 0) {
      if (!isLiteral(thisWord)) {
        if (!isLiteral(otherWord)) {
          // compare two sequences
          // note that they are made up of at least two blocks, and we
          // start comparing from the end, that is at blocks with no
          // (un)set bits
          if (isZeroSequence(thisWord)) {
            if (isOneSequence(otherWord)) {
              // zeros < ones
              return -1;
            }
            // compare two sequences of zeros
            res = getSequenceCount(otherWord) - getSequenceCount(thisWord);
            if (res != 0) {
              return res < 0 ? -1 : 1;
            }
          } else {
            if (isZeroSequence(otherWord)) {
              // ones > zeros
              return 1;
            }
            // compare two sequences of ones
            res = getSequenceCount(thisWord) - getSequenceCount(otherWord);
            if (res != 0) {
              return res < 0 ? -1 : 1;
            }
          }
          // if the sequences are the same (both zeros or both ones)
          // and have the same length, compare the first blocks in the
          // next loop since such blocks might contain (un)set bits
          thisWord = getLiteral(thisWord);
          otherWord = getLiteral(otherWord);
        } else {
          // zeros < literal --> -1
          // ones > literal --> +1
          // note that the sequence is made up of at least two blocks,
          // and we start comparing from the end, that is at a block
          // with no (un)set bits
          if (isZeroSequence(thisWord)) {
            if (otherWord != ConciseSetUtils.ALL_ZEROS_LITERAL) {
              return -1;
            }
          } else {
            if (otherWord != ConciseSetUtils.ALL_ONES_LITERAL) {
              return 1;
            }
          }
          if (getSequenceCount(thisWord) == 1) {
            thisWord = getLiteral(thisWord);
          } else {
            thisWord--;
          }
          if (--otherIndex >= 0) {
            otherWord = other.words[otherIndex];
          }
        }
      } else if (!isLiteral(otherWord)) {
        // literal > zeros --> +1
        // literal < ones --> -1
        // note that the sequence is made up of at least two blocks,
        // and we start comparing from the end, that is at a block
        // with no (un)set bits
        if (isZeroSequence(otherWord)) {
          if (thisWord != ConciseSetUtils.ALL_ZEROS_LITERAL) {
            return 1;
          }
        } else {
          if (thisWord != ConciseSetUtils.ALL_ONES_LITERAL) {
            return -1;
          }
        }
        if (--thisIndex >= 0) {
          thisWord = this.words[thisIndex];
        }
        if (getSequenceCount(otherWord) == 1) {
          otherWord = getLiteral(otherWord);
        } else {
          otherWord--;
        }
      } else {
        res = thisWord - otherWord; // equals getLiteralBits(thisWord) - getLiteralBits(otherWord)
        if (res != 0) {
          return res < 0 ? -1 : 1;
        }
        if (--thisIndex >= 0) {
          thisWord = this.words[thisIndex];
        }
        if (--otherIndex >= 0) {
          otherWord = other.words[otherIndex];
        }
      }
    }
    return thisIndex >= 0 ? 1 : (otherIndex >= 0 ? -1 : 0);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void clear(int from, int to)
  {
    ConciseSet toRemove = empty();
    toRemove.fill(from, to);
    this.removeAll(toRemove);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void fill(int from, int to)
  {
    ConciseSet toAdd = empty();
    toAdd.add(to);
    toAdd.complement();
    toAdd.add(to);

    ConciseSet toRemove = empty();
    toRemove.add(from);
    toRemove.complement();

    toAdd.removeAll(toRemove);

    this.addAll(toAdd);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flip(int e)
  {
    if (!add(e)) {
      remove(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double bitmapCompressionRatio()
  {
    if (isEmpty()) {
      return 0D;
    }
    return (lastWordIndex + 1) / Math.ceil((1 + last) / 32D);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double collectionCompressionRatio()
  {
    if (isEmpty()) {
      return 0D;
    }
    return (double) (lastWordIndex + 1) / size();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String debugInfo()
  {
    final StringBuilder s = new StringBuilder("INTERNAL REPRESENTATION:\n");
    final Formatter f = new Formatter(s, Locale.ENGLISH);

    if (isEmpty()) {
      return s.append("null\n").toString();
    }

    f.format("Elements: %s\n", toString());

    // elements
    int firstBitInWord = 0;
    for (int i = 0; i <= lastWordIndex; i++) {
      // raw representation of words[i]
      f.format("words[%d] = ", i);
      String ws = toBinaryString(words[i]);
      if (isLiteral(words[i])) {
        s.append(ws.substring(0, 1));
        s.append("--");
        s.append(ws.substring(1));
      } else {
        s.append(ws.substring(0, 2));
        s.append('-');
        if (simulateWAH) {
          s.append("xxxxx");
        } else {
          s.append(ws.substring(2, 7));
        }
        s.append('-');
        s.append(ws.substring(7));
      }
      s.append(" --> ");

      // decode words[i]
      if (isLiteral(words[i])) {
        // literal
        s.append("literal: ");
        s.append(toBinaryString(words[i]).substring(1));
        f.format(" ---> [from %d to %d] ", firstBitInWord, firstBitInWord + ConciseSetUtils.MAX_LITERAL_LENGTH - 1);
        firstBitInWord += ConciseSetUtils.MAX_LITERAL_LENGTH;
      } else {
        // sequence
        if (isOneSequence(words[i])) {
          s.append('1');
        } else {
          s.append('0');
        }
        s.append(" block: ");
        s.append(toBinaryString(getLiteralBits(getLiteral(words[i]))).substring(1));
        if (!simulateWAH) {
          s.append(" (bit=");
          int bit = (words[i] & 0x3E000000) >>> 25;
          if (bit == 0) {
            s.append("none");
          } else {
            s.append(StringUtils.format("%4d", bit - 1));
          }
          s.append(')');
        }
        int count = getSequenceCount(words[i]);
        f.format(
            " followed by %d blocks (%d bits)",
            getSequenceCount(words[i]),
            maxLiteralLengthMultiplication(count)
        );
        f.format(
            " ---> [from %d to %d] ",
            firstBitInWord,
            firstBitInWord + (count + 1) * ConciseSetUtils.MAX_LITERAL_LENGTH - 1
        );
        firstBitInWord += (count + 1) * ConciseSetUtils.MAX_LITERAL_LENGTH;
      }
      s.append('\n');
    }

    // object attributes
    f.format("simulateWAH: %b\n", simulateWAH);
    f.format("last: %d\n", last);
    f.format("size: %s\n", (size == -1 ? "invalid" : Integer.toString(size)));
    f.format("words.length: %d\n", words.length);
    f.format("lastWordIndex: %d\n", lastWordIndex);

    // compression
    f.format("bitmap compression: %.2f%%\n", 100D * bitmapCompressionRatio());
    f.format("collection compression: %.2f%%\n", 100D * collectionCompressionRatio());

    return s.toString();
  }

  /**
   * Save the state of the instance to a stream
   */
  private void writeObject(ObjectOutputStream s) throws IOException
  {
    if (words != null && lastWordIndex < words.length - 1) {
      // compact before serializing
      words = Arrays.copyOf(words, lastWordIndex + 1);
    }
    s.defaultWriteObject();
  }

  /**
   * Reconstruct the instance from a stream
   */
  private void readObject(ObjectInputStream s) throws IOException, ClassNotFoundException
  {
    s.defaultReadObject();
    if (words == null) {
      reset();
      return;
    }
    lastWordIndex = words.length - 1;
    updateLast();
    size = -1;
  }

  /**
   * Possible operations
   */
  private enum Operator
  {
    /**
     * @uml.property name="aND"
     * @uml.associationEnd
     */
    AND {
      @Override
      public int combineLiterals(int literal1, int literal2)
      {
        return literal1 & literal2;
      }

      @Override
      public ConciseSet combineEmptySets(ConciseSet op1, ConciseSet op2)
      {
        return op1.empty();
      }

      /** Used to implement {@link #combineDisjointSets(ConciseSet, ConciseSet)} */
      private ConciseSet oneWayCombineDisjointSets(ConciseSet op1, ConciseSet op2)
      {
        // check whether the first operator starts with a sequence that
        // completely "covers" the second operator
        if (isSequenceWithNoBits(op1.words[0])
            && maxLiteralLengthMultiplication(getSequenceCount(op1.words[0]) + 1) > op2.last) {
          // op2 is completely hidden by op1
          if (isZeroSequence(op1.words[0])) {
            return op1.empty();
          }
          // op2 is left unchanged, but the rest of op1 is hidden
          return op2.clone();
        }
        return null;
      }

      @Override
      public ConciseSet combineDisjointSets(ConciseSet op1, ConciseSet op2)
      {
        ConciseSet res = oneWayCombineDisjointSets(op1, op2);
        if (res == null) {
          res = oneWayCombineDisjointSets(op2, op1);
        }
        return res;
      }
    },

    /**
     * @uml.property name="oR"
     * @uml.associationEnd
     */
    OR {
      @Override
      public int combineLiterals(int literal1, int literal2)
      {
        return literal1 | literal2;
      }

      @Override
      public ConciseSet combineEmptySets(ConciseSet op1, ConciseSet op2)
      {
        if (!op1.isEmpty()) {
          return op1.clone();
        }
        if (!op2.isEmpty()) {
          return op2.clone();
        }
        return op1.empty();
      }

      /** Used to implement {@link #combineDisjointSets(ConciseSet, ConciseSet)} */
      private ConciseSet oneWayCombineDisjointSets(ConciseSet op1, ConciseSet op2)
      {
        // check whether the first operator starts with a sequence that
        // completely "covers" the second operator
        if (isSequenceWithNoBits(op1.words[0])
            && maxLiteralLengthMultiplication(getSequenceCount(op1.words[0]) + 1) > op2.last) {
          // op2 is completely hidden by op1
          if (isOneSequence(op1.words[0])) {
            return op1.clone();
          }
          // op2 is left unchanged, but the rest of op1 must be appended...

          // ... first, allocate sufficient space for the result
          ConciseSet res = op1.empty();
          res.words = new int[op1.lastWordIndex + op2.lastWordIndex + 3];
          res.lastWordIndex = op2.lastWordIndex;

          // ... then, copy op2
          System.arraycopy(op2.words, 0, res.words, 0, op2.lastWordIndex + 1);

          // ... finally, append op1
          WordIterator wordIterator = op1.new WordIterator();
          wordIterator.prepareNext(maxLiteralLengthDivision(op2.last) + 1);
          wordIterator.flush(res);
          if (op1.size < 0 || op2.size < 0) {
            res.size = -1;
          } else {
            res.size = op1.size + op2.size;
          }
          res.last = op1.last;
          res.compact();
          return res;
        }
        return null;
      }

      @Override
      public ConciseSet combineDisjointSets(ConciseSet op1, ConciseSet op2)
      {
        ConciseSet res = oneWayCombineDisjointSets(op1, op2);
        if (res == null) {
          res = oneWayCombineDisjointSets(op2, op1);
        }
        return res;
      }
    },

    /**
     * @uml.property name="xOR"
     * @uml.associationEnd
     */
    XOR {
      @Override
      public int combineLiterals(int literal1, int literal2)
      {
        return ConciseSetUtils.ALL_ZEROS_LITERAL | (literal1 ^ literal2);
      }

      @Override
      public ConciseSet combineEmptySets(ConciseSet op1, ConciseSet op2)
      {
        if (!op1.isEmpty()) {
          return op1.clone();
        }
        if (!op2.isEmpty()) {
          return op2.clone();
        }
        return op1.empty();
      }

      /** Used to implement {@link #combineDisjointSets(ConciseSet, ConciseSet)} */
      private ConciseSet oneWayCombineDisjointSets(ConciseSet op1, ConciseSet op2)
      {
        // check whether the first operator starts with a sequence that
        // completely "covers" the second operator
        if (isSequenceWithNoBits(op1.words[0])
            && maxLiteralLengthMultiplication(getSequenceCount(op1.words[0]) + 1) > op2.last) {
          // op2 is left unchanged by op1
          if (isZeroSequence(op1.words[0])) {
            return OR.combineDisjointSets(op1, op2);
          }
          // op2 must be complemented, then op1 must be appended
          // it is better to perform it normally...
          return null;
        }
        return null;
      }

      @Override
      public ConciseSet combineDisjointSets(ConciseSet op1, ConciseSet op2)
      {
        ConciseSet res = oneWayCombineDisjointSets(op1, op2);
        if (res == null) {
          res = oneWayCombineDisjointSets(op2, op1);
        }
        return res;
      }
    },

    /**
     * @uml.property name="aNDNOT"
     * @uml.associationEnd
     */
    ANDNOT {
      @Override
      public int combineLiterals(int literal1, int literal2)
      {
        return ConciseSetUtils.ALL_ZEROS_LITERAL | (literal1 & (~literal2));
      }

      @Override
      public ConciseSet combineEmptySets(ConciseSet op1, ConciseSet op2)
      {
        if (!op1.isEmpty()) {
          return op1.clone();
        }
        return op1.empty();
      }

      @Override
      public ConciseSet combineDisjointSets(ConciseSet op1, ConciseSet op2)
      {
        // check whether the first operator starts with a sequence that
        // completely "covers" the second operator
        if (isSequenceWithNoBits(op1.words[0])
            && maxLiteralLengthMultiplication(getSequenceCount(op1.words[0]) + 1) > op2.last) {
          // op1 is left unchanged by op2
          if (isZeroSequence(op1.words[0])) {
            return op1.clone();
          }
          // op2 must be complemented, then op1 must be appended
          // it is better to perform it normally...
          return null;
        }
        // check whether the second operator starts with a sequence that
        // completely "covers" the first operator
        if (isSequenceWithNoBits(op2.words[0])
            && maxLiteralLengthMultiplication(getSequenceCount(op2.words[0]) + 1) > op1.last) {
          // op1 is left unchanged by op2
          if (isZeroSequence(op2.words[0])) {
            return op1.clone();
          }
          // op1 is cleared by op2
          return op1.empty();
        }
        return null;
      }
    };

    /**
     * Performs the operation on the given literals
     *
     * @param literal1 left operand
     * @param literal2 right operand
     *
     * @return literal representing the result of the specified operation
     */
    public abstract int combineLiterals(int literal1, int literal2);

    /**
     * Performs the operation when one or both operands are empty set
     * <p/>
     * <b>NOTE: the caller <i>MUST</i> assure that one or both the operands
     * are empty!!!</b>
     *
     * @param op1 left operand
     * @param op2 right operand
     *
     * @return <code>null</code> if both operands are non-empty
     */
    public abstract ConciseSet combineEmptySets(ConciseSet op1, ConciseSet op2);

    /**
     * Performs the operation in the special case of "disjoint" sets, namely
     * when the first (or the second) operand starts with a sequence (it
     * does not matter if 0's or 1's) that completely covers all the bits of
     * the second (or the first) operand.
     *
     * @param op1 left operand
     * @param op2 right operand
     *
     * @return <code>null</code> if operands are non-disjoint
     */
    public abstract ConciseSet combineDisjointSets(ConciseSet op1, ConciseSet op2);
  }

  /**
   * Iterator over the bits of a single literal/fill word
   */
  private interface WordExpander
  {
    public boolean hasNext();

    public boolean hasPrevious();

    public int next();

    public int previous();

    public void skipAllAfter(int i);

    public void skipAllBefore(int i);

    public void reset(int offset, int word, boolean fromBeginning);
  }

  /**
   * Iterates over words, from the rightmost (LSB) to the leftmost (MSB).
   * <p/>
   * When {@link ConciseSet#simulateWAH} is <code>false</code>, mixed
   * sequences are "broken" into a literal (i.e., the first block is coded
   * with a literal in {@link #word}) and a "pure" sequence (i.e., the
   * remaining blocks are coded with a sequence with no bits in {@link #word})
   */
  private class WordIterator
  {
    /**
     * copy of the current word
     */
    int word;

    /**
     * current word index
     */
    int index;

    /**
     * <code>true</code> if {@link #word} is a literal
     */
    boolean isLiteral;

    /**
     * number of blocks in the current word (1 for literals, > 1 for sequences)
     */
    int count;

    /**
     * Initialize data
     */
    WordIterator()
    {
      isLiteral = false;
      index = -1;
      prepareNext();
    }

    /**
     * @return <code>true</code> if there is no current word
     */
    boolean exhausted()
    {
      return index > lastWordIndex;
    }

    /**
     * Prepare the next value for {@link #word} after skipping a given
     * number of 31-bit blocks in the current sequence.
     * <p/>
     * <b>NOTE:</b> it works only when the current word is within a
     * sequence, namely a literal cannot be skipped. Moreover, the number of
     * blocks to skip must be less than the remaining blocks in the current
     * sequence.
     *
     * @param c number of 31-bit "blocks" to skip
     *
     * @return <code>false</code> if the next word does not exists
     */
    boolean prepareNext(int c)
    {
      assert c <= count;
      count -= c;
      if (count == 0) {
        return prepareNext();
      }
      return true;
    }

    /**
     * Prepare the next value for {@link #word}
     *
     * @return <code>false</code> if the next word does not exists
     */
    boolean prepareNext()
    {
      if (!simulateWAH && isLiteral && count > 1) {
        count--;
        isLiteral = false;
        word = getSequenceWithNoBits(words[index]) - 1;
        return true;
      }

      index++;
      if (index > lastWordIndex) {
        return false;
      }
      word = words[index];
      isLiteral = isLiteral(word);
      if (!isLiteral) {
        count = getSequenceCount(word) + 1;
        if (!simulateWAH && !isSequenceWithNoBits(word)) {
          isLiteral = true;
          int bit = (1 << (word >>> 25)) >>> 1;
          word = isZeroSequence(word)
                 ? (ConciseSetUtils.ALL_ZEROS_LITERAL | bit)
                 : (ConciseSetUtils.ALL_ONES_LITERAL & ~bit);
        }
      } else {
        count = 1;
      }
      return true;
    }

    /**
     * @return the literal word corresponding to each block contained in the
     * current sequence word. Not to be used with literal words!
     */
    int toLiteral()
    {
      assert !isLiteral;
      return ConciseSetUtils.ALL_ZEROS_LITERAL | ((word << 1) >> ConciseSetUtils.MAX_LITERAL_LENGTH);
    }

    /**
     * Copies all the remaining words in the given set
     *
     * @param s set where the words must be copied
     *
     * @return <code>false</code> if there are no words to copy
     */
    private boolean flush(ConciseSet s)
    {
      // nothing to flush
      if (exhausted()) {
        return false;
      }

      // try to "compress" the first few words
      do {
        if (isLiteral) {
          s.appendLiteral(word);
        } else {
          s.appendFill(count, word);
        }
      } while (prepareNext() && s.words[s.lastWordIndex] != word);

      // copy remaining words "as-is"
      int delta = lastWordIndex - index + 1;
      System.arraycopy(words, index, s.words, s.lastWordIndex + 1, delta);
      s.lastWordIndex += delta;
      s.last = last;
      return true;
    }
  }

  /*
    * DEBUGGING METHODS
    */

  /**
   * Iterator over the bits of literal and zero-fill words
   */
  private class LiteralAndZeroFillExpander implements WordExpander
  {
    final int[] buffer = new int[ConciseSetUtils.MAX_LITERAL_LENGTH];
    int len = 0;
    int current = 0;

    @Override
    public boolean hasNext()
    {
      return current < len;
    }

    @Override
    public boolean hasPrevious()
    {
      return current > 0;
    }

    @Override
    public int next()
    {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return buffer[current++];
    }

    @Override
    public int previous()
    {
      if (!hasPrevious()) {
        throw new NoSuchElementException();
      }
      return buffer[--current];
    }

    @Override
    public void skipAllAfter(int i)
    {
      while (hasPrevious() && buffer[current - 1] > i) {
        current--;
      }
    }

    @Override
    public void skipAllBefore(int i)
    {
      while (hasNext() && buffer[current] < i) {
        current++;
      }
    }

    @Override
    public void reset(int offset, int word, boolean fromBeginning)
    {
      if (isLiteral(word)) {
        len = 0;
        for (int i = 0; i < ConciseSetUtils.MAX_LITERAL_LENGTH; i++) {
          if ((word & (1 << i)) != 0) {
            buffer[len++] = offset + i;
          }
        }
        current = fromBeginning ? 0 : len;
      } else {
        if (isZeroSequence(word)) {
          if (simulateWAH || isSequenceWithNoBits(word)) {
            len = 0;
            current = 0;
          } else {
            len = 1;
            buffer[0] = offset + ((0x3FFFFFFF & word) >>> 25) - 1;
            current = fromBeginning ? 0 : 1;
          }
        } else {
          throw new RuntimeException("sequence of ones!");
        }
      }
    }
  }

  /**
   * Iterator over the bits of one-fill words
   */
  private class OneFillExpander implements WordExpander
  {
    int firstInt = 1;
    int lastInt = -1;
    int current = 0;
    int exception = -1;

    @Override
    public boolean hasNext()
    {
      return current < lastInt;
    }

    @Override
    public boolean hasPrevious()
    {
      return current > firstInt;
    }

    @Override
    public int next()
    {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      current++;
      if (!simulateWAH && current == exception) {
        current++;
      }
      return current;
    }

    @Override
    public int previous()
    {
      if (!hasPrevious()) {
        throw new NoSuchElementException();
      }
      current--;
      if (!simulateWAH && current == exception) {
        current--;
      }
      return current;
    }

    @Override
    public void skipAllAfter(int i)
    {
      if (i >= current) {
        return;
      }
      current = i + 1;
    }

    @Override
    public void skipAllBefore(int i)
    {
      if (i <= current) {
        return;
      }
      current = i - 1;
    }

    @Override
    public void reset(int offset, int word, boolean fromBeginning)
    {
      if (!isOneSequence(word)) {
        throw new RuntimeException("NOT a sequence of ones!");
      }
      firstInt = offset;
      lastInt = offset + maxLiteralLengthMultiplication(getSequenceCount(word) + 1) - 1;
      if (!simulateWAH) {
        exception = offset + ((0x3FFFFFFF & word) >>> 25) - 1;
        if (exception == firstInt) {
          firstInt++;
        }
        if (exception == lastInt) {
          lastInt--;
        }
      }
      current = fromBeginning ? (firstInt - 1) : (lastInt + 1);
    }
  }

  /**
   * Iterator for all the integers of a  {@link ConciseSet}  instance
   */
  private class BitIterator implements IntIterator
  {
    /**
     * @uml.property name="litExp"
     * @uml.associationEnd
     */
    final LiteralAndZeroFillExpander litExp = new LiteralAndZeroFillExpander();
    /**
     * @uml.property name="oneExp"
     * @uml.associationEnd
     */
    final OneFillExpander oneExp = new OneFillExpander();
    /**
     * @uml.property name="exp"
     * @uml.associationEnd
     */
    WordExpander exp;
    int nextIndex = 0;
    int nextOffset = 0;

    private BitIterator()
    {
      nextWord();
    }

    private void nextWord()
    {
      final int word = words[nextIndex++];
      exp = isOneSequence(word) ? oneExp : litExp;
      exp.reset(nextOffset, word, true);

      // prepare next offset
      if (isLiteral(word)) {
        nextOffset += ConciseSetUtils.MAX_LITERAL_LENGTH;
      } else {
        nextOffset += maxLiteralLengthMultiplication(getSequenceCount(word) + 1);
      }
    }

    @Override
    public boolean hasNext()
    {
      return nextIndex <= lastWordIndex || exp.hasNext();
    }

    @Override
    public int next()
    {
      while (!exp.hasNext()) {
        if (nextIndex > lastWordIndex) {
          throw new NoSuchElementException();
        }
        nextWord();
      }
      return exp.next();
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public void skipAllBefore(int element)
    {
      while (true) {
        exp.skipAllBefore(element);
        if (exp.hasNext() || nextIndex > lastWordIndex) {
          return;
        }
        nextWord();
      }
    }

    @Override
    public IntIterator clone()
    {
      BitIterator retVal = new BitIterator();
      retVal.exp = exp;
      retVal.nextIndex = nextIndex;
      retVal.nextOffset = nextOffset;
      return retVal;
    }
  }

  /**
   * @author alessandrocolantonio
   */
  private class ReverseBitIterator implements IntIterator
  {
    /**
     * @uml.property name="litExp"
     * @uml.associationEnd
     */
    final LiteralAndZeroFillExpander litExp = new LiteralAndZeroFillExpander();
    /**
     * @uml.property name="oneExp"
     * @uml.associationEnd
     */
    final OneFillExpander oneExp = new OneFillExpander();
    /**
     * @uml.property name="exp"
     * @uml.associationEnd
     */
    WordExpander exp;
    int nextIndex = lastWordIndex;
    int nextOffset = maxLiteralLengthMultiplication(maxLiteralLengthDivision(last) + 1);
    int firstIndex; // first non-zero block

    ReverseBitIterator()
    {
      // identify the first non-zero block
      if ((isSequenceWithNoBits(words[0]) && isZeroSequence(words[0])) || (isLiteral(words[0])
                                                                           && words[0]
                                                                              == ConciseSetUtils.ALL_ZEROS_LITERAL)) {
        firstIndex = 1;
      } else {
        firstIndex = 0;
      }
      previousWord();
    }

    void previousWord()
    {
      final int word = words[nextIndex--];
      exp = isOneSequence(word) ? oneExp : litExp;
      if (isLiteral(word)) {
        nextOffset -= ConciseSetUtils.MAX_LITERAL_LENGTH;
      } else {
        nextOffset -= maxLiteralLengthMultiplication(getSequenceCount(word) + 1);
      }
      exp.reset(nextOffset, word, false);
    }

    @Override
    public boolean hasNext()
    {
      return nextIndex >= firstIndex || exp.hasPrevious();
    }

    @Override
    public int next()
    {
      while (!exp.hasPrevious()) {
        if (nextIndex < firstIndex) {
          throw new NoSuchElementException();
        }
        previousWord();
      }
      return exp.previous();
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public void skipAllBefore(int element)
    {
      while (true) {
        exp.skipAllAfter(element);
        if (exp.hasPrevious() || nextIndex < firstIndex) {
          return;
        }
        previousWord();
      }
    }

    @Override
    public IntIterator clone()
    {
      ReverseBitIterator retVal = new ReverseBitIterator();
      retVal.exp = exp;
      retVal.nextIndex = nextIndex;
      retVal.nextOffset = nextOffset;
      retVal.firstIndex = firstIndex;
      return retVal;
    }
  }
}
