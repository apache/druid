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


import io.druid.extendedset.utilities.BitCount;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Formatter;
import java.util.Locale;
import java.util.NoSuchElementException;

/**
 * An {@link IntSet} implementation, representing a set of integers, based on an
 * uncompressed bitmap.
 * <p/>
 * It actually is an extension of {@link BitSet}. More specifically, union and
 * intersection operations are mainly derived from the code of {@link BitSet} to
 * provide bitwise "or" and "and".
 * <p/>
 * The iterator implemented for this class allows for modifications during the
 * iteration, that is it is possible to add/remove elements through
 * {@link #add(int)}, {@link #remove(int)}, {@link #addAll(IntSet)},
 * {@link #removeAll(IntSet)}, {@link #retainAll(IntSet)}, etc.. In this case,
 * {@link IntIterator#next()} returns the first integral greater than the last
 * visited one.
 *
 * @author Alessandro Colantonio
 * @version $Id$
 */
public class FastSet extends AbstractIntSet implements java.io.Serializable
{
  /**
   * generated serial ID
   */
  private static final long serialVersionUID = 6519808981110513440L;

  /**
   * number of bits within each word
   */
  private final static int WORD_SIZE = 32;

  /**
   * 32-bit string of all 1's
   */
  private static final int ALL_ONES_WORD = 0xFFFFFFFF;

  /**
   * all bits, grouped in blocks of length 32
   */
  private int[] words;

  /**
   * index of the first empty word, that is the number of words in the logical
   * size of this {@link FastSet}
   */
  private transient int firstEmptyWord;

  /**
   * cached set size (only for fast size() call). When -1, the cache is invalid
   */
  private transient int size;

  /**
   * Creates a new, empty set.
   */
  public FastSet()
  {
    clear();
  }

  /**
   * Creates a new, empty set. It preallocates the space for
   * <code>maxWordsInUse</code> words.
   */
  private FastSet(int wordsToAllocate)
  {
    firstEmptyWord = 0;
    size = 0;
    words = new int[wordsToAllocate];
  }

  /**
   * Given a number, it returns the multiplication by the number of bits for each block
   */
  private static int multiplyByWordSize(int i)
  {
    return i << 5; // i * WORD_SIZE;
  }

  /**
   * Given a bit index, it returns the index of the word containing it
   */
  private static int wordIndex(int bitIndex)
  {
    if (bitIndex < 0) {
      throw new IndexOutOfBoundsException("index < 0: " + bitIndex);
    }
    return bitIndex >> 5;
  }

  /**
   * Given a bit index, it returns the index of the word containing it
   */
  private static int wordIndexNoCheck(int bitIndex)
  {
    return bitIndex >> 5;
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
   * Sets the field {@link #firstEmptyWord} with the logical size in words of the
   * bit set.
   */
  private void fixFirstEmptyWord()
  {
    int i = firstEmptyWord - 1;
    final int[] localWords = words; // faster
    while (i >= 0 && localWords[i] == 0) {
      i--;
    }
    firstEmptyWord = i + 1;
  }

  /**
   * Ensures that the {@link FastSet} can hold enough words.
   *
   * @param wordsRequired the minimum acceptable number of words.
   */
  private void ensureCapacity(int wordsRequired)
  {
    if (words.length >= wordsRequired) {
      return;
    }
    int newLength = Math.max(words.length << 1, wordsRequired);
    words = Arrays.copyOf(words, newLength);
  }

  /**
   * Ensures that the {@link FastSet} can accommodate a given word index
   *
   * @param wordIndex the index to be accommodated.
   */
  private void expandTo(int wordIndex)
  {
    int wordsRequired = wordIndex + 1;
    if (firstEmptyWord < wordsRequired) {
      ensureCapacity(wordsRequired);
      firstEmptyWord = wordsRequired;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public FastSet clone()
  {
    // NOTE: do not use super.clone() since it is 10 times slower!
    FastSet res = new FastSet();
    res.firstEmptyWord = firstEmptyWord;
    res.size = size;
    res.words = Arrays.copyOf(words, firstEmptyWord);
    return res;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode()
  {
    int h = 1;
    final int[] localWords = words; // faster
    for (int i = 0; i < firstEmptyWord; i++) {
      h = (h << 5) - h + localWords[i];
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
    if (!(obj instanceof FastSet)) {
      return super.equals(obj);
    }

    final FastSet other = (FastSet) obj;
    if (firstEmptyWord != other.firstEmptyWord) {
      return false;
    }
    final int[] localWords = words;       // faster
    final int[] localOtherWords = other.words;   // faster
    for (int i = 0; i < firstEmptyWord; i++) {
      if (localWords[i] != localOtherWords[i]) {
        return false;
      }
    }
    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isEmpty()
  {
    return firstEmptyWord == 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int size()
  {
    // check if the cached size is invalid
    if (size < 0) {
      size = BitCount.count(words, firstEmptyWord);
    }
    return size;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean add(int i)
  {
    int wordIndex = wordIndex(i);
    expandTo(wordIndex);
    int before = words[wordIndex];
    words[wordIndex] |= (1 << i);
    if (before != words[wordIndex]) {
      if (size >= 0) {
        size++;
      }
      return true;
    }
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean remove(int i)
  {
    if (i < 0) {
      return false;
    }

    int wordIndex = wordIndex(i);
    if (wordIndex >= firstEmptyWord) {
      return false;
    }
    int before = words[wordIndex];
    words[wordIndex] &= ~(1 << i);
    if (before != words[wordIndex]) {
      if (size >= 0) {
        size--;
      }
      fixFirstEmptyWord();
      return true;
    }
    return false;
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

    final FastSet other = convert(c);

    int wordsInCommon = Math.min(firstEmptyWord, other.firstEmptyWord);

    boolean modified = false;
    if (firstEmptyWord < other.firstEmptyWord) {
      modified = true;
      ensureCapacity(other.firstEmptyWord);
      firstEmptyWord = other.firstEmptyWord;
    }

    final int[] localWords = words;       // faster
    final int[] localOtherWords = other.words;   // faster

    // Perform logical OR on words in common
    for (int i = 0; i < wordsInCommon; i++) {
      int before = localWords[i];
      localWords[i] |= localOtherWords[i];
      modified = modified || before != localWords[i];
    }

    // Copy any remaining words
    if (wordsInCommon < other.firstEmptyWord) {
      modified = true;
      System.arraycopy(
          other.words, wordsInCommon, words,
          wordsInCommon, firstEmptyWord - wordsInCommon
      );
    }
    if (modified) {
      size = -1;
    }
    return modified;
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

    final FastSet other = convert(c);
    final int[] localWords = words;       // faster
    final int[] localOtherWords = other.words;   // faster

    // Perform logical (a & !b) on words in common
    boolean modified = false;
    for (int i = Math.min(firstEmptyWord, other.firstEmptyWord) - 1; i >= 0; i--) {
      int before = localWords[i];
      localWords[i] &= ~localOtherWords[i];
      modified = modified || before != localWords[i];
    }
    if (modified) {
      fixFirstEmptyWord();
      size = -1;
    }
    return modified;
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

    final FastSet other = convert(c);
    final int[] localWords = words;       // faster
    final int[] localOtherWords = other.words;   // faster

    boolean modified = false;
    if (firstEmptyWord > other.firstEmptyWord) {
      modified = true;
      while (firstEmptyWord > other.firstEmptyWord) {
        localWords[--firstEmptyWord] = 0;
      }
    }

    // Perform logical AND on words in common
    for (int i = 0; i < firstEmptyWord; i++) {
      int before = localWords[i];
      localWords[i] &= localOtherWords[i];
      modified = modified || before != localWords[i];
    }
    if (modified) {
      fixFirstEmptyWord();
      size = -1;
    }
    return modified;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void clear()
  {
    words = new int[10];
    firstEmptyWord = 0;
    size = 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean contains(int i)
  {
    if (isEmpty() || i < 0) {
      return false;
    }
    int wordIndex = wordIndexNoCheck(i);
    return (wordIndex < firstEmptyWord)
           && ((words[wordIndex] & (1 << i)) != 0);
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

    final FastSet other = convert(c);

    if (other.firstEmptyWord > firstEmptyWord) {
      return false;
    }

    final int[] localWords = words;       // faster
    final int[] localOtherWords = other.words;   // faster
    for (int i = 0; i < other.firstEmptyWord; i++) {
      int o = localOtherWords[i];
      if ((localWords[i] & o) != o) {
        return false;
      }
    }
    return true;
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

    final FastSet other = convert(c);
    final int[] localWords = words;       // faster
    final int[] localOtherWords = other.words;   // faster

    int count = 0;
    for (int i = Math.min(firstEmptyWord, other.firstEmptyWord) - 1; i >= 0; i--) {
      count += BitCount.count(localWords[i] & localOtherWords[i]);
      if (count >= minElements) {
        return true;
      }
    }
    return false;
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

    final FastSet other = convert(c);
    final int[] localWords = words;       // faster
    final int[] localOtherWords = other.words;   // faster

    for (int i = Math.min(firstEmptyWord, other.firstEmptyWord) - 1; i >= 0; i--) {
      if ((localWords[i] & localOtherWords[i]) != 0) {
        return true;
      }
    }
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int intersectionSize(IntSet c)
  {
    if (c == null || c.isEmpty()) {
      return 0;
    }
    if (c == this) {
      return size();
    }
    if (isEmpty()) {
      return 0;
    }

    final FastSet other = convert(c);
    final int[] localWords = words;       // faster
    final int[] localOtherWords = other.words;   // faster

    int count = 0;
    for (int i = Math.min(firstEmptyWord, other.firstEmptyWord) - 1; i >= 0; i--) {
      count += BitCount.count(localWords[i] & localOtherWords[i]);
    }
    return count;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IntIterator iterator()
  {
    return new BitIterator();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IntIterator descendingIterator()
  {
    return new ReverseBitIterator();
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
    return multiplyByWordSize(firstEmptyWord - 1)
           + (WORD_SIZE - Integer.numberOfLeadingZeros(words[firstEmptyWord - 1])) - 1;
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
    if (size > 0) {
      size = last() - size + 1;
    }
    int lastWordMask = ALL_ONES_WORD >>> Integer.numberOfLeadingZeros(words[firstEmptyWord - 1]);
    final int[] localWords = words;       // faster
    for (int i = 0; i < firstEmptyWord - 1; i++) {
      localWords[i] ^= ALL_ONES_WORD;
    }
    localWords[firstEmptyWord - 1] ^= lastWordMask;
    fixFirstEmptyWord();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public FastSet complemented()
  {
    FastSet clone = clone();
    clone.complement();
    return clone;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public FastSet empty()
  {
    return new FastSet();
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
    return 1D;
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
    return (double) firstEmptyWord / size();
  }

  /**
   * Convert a given collection to a {@link FastSet} instance
   */
  private FastSet convert(IntSet c)
  {
    if (c instanceof FastSet) {
      return (FastSet) c;
    }
    if (c == null) {
      return new FastSet();
    }

    FastSet res = new FastSet();
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
  public FastSet convert(Collection<Integer> c)
  {
    FastSet res = empty();
    if (c != null) {
      for (int i : c) {
        res.add(i);
      }
    }
    return res;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public FastSet convert(int... a)
  {
    FastSet res = new FastSet();
    if (a != null) {
      for (int i : a) {
        res.add(i);
      }
    }
    return res;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void fill(int fromIndex, int toIndex)
  {
    if (fromIndex > toIndex) {
      throw new IndexOutOfBoundsException(
          "fromIndex: " + fromIndex
          + " > toIndex: " + toIndex
      );
    }
    if (fromIndex == toIndex) {
      add(fromIndex);
      return;
    }

    // Increase capacity if necessary
    int startWordIndex = wordIndex(fromIndex);
    int endWordIndex = wordIndex(toIndex);
    expandTo(endWordIndex);

    final int[] localWords = words;       // faster

    boolean modified = false;
    int firstWordMask = ALL_ONES_WORD << fromIndex;
    int lastWordMask = ALL_ONES_WORD >>> -(toIndex + 1);
    if (startWordIndex == endWordIndex) {
      // Case 1: One word
      int before = localWords[startWordIndex];
      localWords[startWordIndex] |= (firstWordMask & lastWordMask);
      modified = localWords[startWordIndex] != before;
    } else {
      // Case 2: Multiple words
      // Handle first word
      int before = localWords[startWordIndex];
      localWords[startWordIndex] |= firstWordMask;
      modified = localWords[startWordIndex] != before;

      // Handle intermediate words, if any
      for (int i = startWordIndex + 1; i < endWordIndex; i++) {
        modified = modified || localWords[i] != ALL_ONES_WORD;
        localWords[i] = ALL_ONES_WORD;
      }

      // Handle last word
      before = localWords[endWordIndex];
      localWords[endWordIndex] |= lastWordMask;
      modified = modified || localWords[endWordIndex] != before;
    }
    if (modified) {
      size = -1;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void clear(int fromIndex, int toIndex)
  {
    if (fromIndex > toIndex) {
      throw new IndexOutOfBoundsException(
          "fromIndex: " + fromIndex
          + " > toIndex: " + toIndex
      );
    }
    if (fromIndex == toIndex) {
      remove(fromIndex);
      return;
    }

    int startWordIndex = wordIndex(fromIndex);
    if (startWordIndex >= firstEmptyWord) {
      return;
    }

    int endWordIndex = wordIndex(toIndex);
    if (endWordIndex >= firstEmptyWord) {
      toIndex = last();
      endWordIndex = firstEmptyWord - 1;
    }

    final int[] localWords = words;       // faster

    boolean modified = false;
    int firstWordMask = ALL_ONES_WORD << fromIndex;
    int lastWordMask = ALL_ONES_WORD >>> -(toIndex + 1);
    if (startWordIndex == endWordIndex) {
      // Case 1: One word
      int before = localWords[startWordIndex];
      localWords[startWordIndex] &= ~(firstWordMask & lastWordMask);
      modified = localWords[startWordIndex] != before;
    } else {
      // Case 2: Multiple words
      // Handle first word
      int before = localWords[startWordIndex];
      localWords[startWordIndex] &= ~firstWordMask;
      modified = localWords[startWordIndex] != before;

      // Handle intermediate words, if any
      for (int i = startWordIndex + 1; i < endWordIndex; i++) {
        modified = modified || localWords[i] != 0;
        localWords[i] = 0;
      }

      // Handle last word
      before = localWords[endWordIndex];
      localWords[endWordIndex] &= ~lastWordMask;
      modified = modified || localWords[endWordIndex] != before;
    }
    if (modified) {
      fixFirstEmptyWord();
      size = -1;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flip(int e)
  {
    int wordIndex = wordIndex(e);
    expandTo(wordIndex);
    int mask = (1 << e);
    words[wordIndex] ^= mask;
    fixFirstEmptyWord();
    if (size >= 0) {
      if ((words[wordIndex] & mask) == 0) {
        size--;
      } else {
        size++;
      }
    }
  }

  /**
   * {@inheritDoc}
   */
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

    final FastSet other = convert(o);
    final int[] localWords = words;       // faster
    final int[] localOtherWords = other.words;   // faster

    if (firstEmptyWord > other.firstEmptyWord) {
      return 1;
    }
    if (firstEmptyWord < other.firstEmptyWord) {
      return -1;
    }
    for (int i = firstEmptyWord - 1; i >= 0; i--) {
      long w1 = localWords[i] & 0xFFFFFFFFL;
      long w2 = localOtherWords[i] & 0xFFFFFFFFL;
      int res = w1 < w2 ? -1 : (w1 > w2 ? 1 : 0);
      if (res != 0) {
        return res;
      }
    }
    return 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int get(int index)
  {
    if (index < 0) {
      throw new IndexOutOfBoundsException();
    }

    int count = 0;
    final int[] localWords = words;       // faster
    for (int j = 0; j < firstEmptyWord; j++) {
      int w = localWords[j];
      int current = BitCount.count(w);
      if (index < count + current) {
        int bit = -1;
        for (int skip = index - count; skip >= 0; skip--) {
          bit = Integer.numberOfTrailingZeros(w & (ALL_ONES_WORD << (bit + 1)));
        }
        return multiplyByWordSize(j) + bit;
      }
      count += current;
    }
    throw new NoSuchElementException();
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

    int index = wordIndex(e);
    if (index >= firstEmptyWord || (words[index] & (1 << e)) == 0) {
      return -1;
    }
    int count = BitCount.count(words, index);
    count += BitCount.count(words[index] & ~(ALL_ONES_WORD << e));
    return count;

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public FastSet intersection(IntSet other)
  {
    if (isEmpty() || other == null || other.isEmpty()) {
      return empty();
    }
    if (other == this) {
      return clone();
    }

    final FastSet o = convert(other);
    FastSet res = new FastSet(Math.min(firstEmptyWord, o.firstEmptyWord));
    res.firstEmptyWord = res.words.length;

    final int[] localWords = words;     // faster
    final int[] localOtherWords = o.words;   // faster
    final int[] localResWords = res.words;   // faster

    for (int i = 0; i < res.firstEmptyWord; i++) {
      localResWords[i] = localWords[i] & localOtherWords[i];
    }
    res.fixFirstEmptyWord();
    res.size = -1;
    return res;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public FastSet union(IntSet other)
  {
    if (other == null || other.isEmpty() || this == other) {
      return clone();
    }

    final FastSet o = convert(other);
    if (isEmpty()) {
      return o.clone();
    }

    FastSet res = new FastSet(Math.max(firstEmptyWord, o.firstEmptyWord));
    res.firstEmptyWord = res.words.length;
    final int wordsInCommon = Math.min(firstEmptyWord, o.firstEmptyWord);

    final int[] localWords = words;     // faster
    final int[] localOtherWords = o.words;   // faster
    final int[] localResWords = res.words;   // faster

    for (int i = 0; i < wordsInCommon; i++) {
      localResWords[i] = localWords[i] | localOtherWords[i];
    }

    if (wordsInCommon < firstEmptyWord) {
      System.arraycopy(
          localWords, wordsInCommon, localResWords, wordsInCommon,
          res.firstEmptyWord - wordsInCommon
      );
    }
    if (wordsInCommon < o.firstEmptyWord) {
      System.arraycopy(
          localOtherWords, wordsInCommon, localResWords, wordsInCommon,
          res.firstEmptyWord - wordsInCommon
      );
    }
    res.size = -1;
    return res;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public FastSet difference(IntSet other)
  {
    if (other == null || other.isEmpty()) {
      return clone();
    }
    if (other == this || isEmpty()) {
      return empty();
    }

    final FastSet o = convert(other);
    FastSet res = new FastSet(firstEmptyWord);
    res.firstEmptyWord = firstEmptyWord;

    final int[] localWords = words;     // faster
    final int[] localOtherWords = o.words;   // faster
    final int[] localResWords = res.words;   // faster

    int i = 0;
    final int m = Math.min(firstEmptyWord, o.firstEmptyWord);
    for (; i < m; i++) {
      localResWords[i] = localWords[i] & ~localOtherWords[i];
    }
    if (i < firstEmptyWord) {
      System.arraycopy(localWords, i, localResWords, i, firstEmptyWord - i);
    } else {
      res.fixFirstEmptyWord();
    }
    res.size = -1;
    return res;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public FastSet symmetricDifference(IntSet other)
  {
    if (other == null || other.isEmpty()) {
      return clone();
    }
    if (other == this) {
      return empty();
    }

    final FastSet o = convert(other);
    if (isEmpty()) {
      return o.clone();
    }

    FastSet res = new FastSet(Math.max(firstEmptyWord, o.firstEmptyWord));
    res.firstEmptyWord = res.words.length;
    final int wordsInCommon = Math.min(firstEmptyWord, o.firstEmptyWord);

    final int[] localWords = words;     // faster
    final int[] localOtherWords = o.words;   // faster
    final int[] localResWords = res.words;   // faster

    for (int i = 0; i < wordsInCommon; i++) {
      localResWords[i] = localWords[i] ^ localOtherWords[i];
    }

    if (wordsInCommon < firstEmptyWord) {
      System.arraycopy(
          localWords, wordsInCommon, localResWords, wordsInCommon,
          res.firstEmptyWord - wordsInCommon
      );
    } else if (wordsInCommon < o.firstEmptyWord) {
      System.arraycopy(
          localOtherWords, wordsInCommon, localResWords, wordsInCommon,
          res.firstEmptyWord - wordsInCommon
      );
    } else {
      res.fixFirstEmptyWord();
    }
    res.size = -1;
    return res;
  }

  /**
   * Save the state of the {@link ConciseSet}instance to a stream
   */
  private void writeObject(ObjectOutputStream s) throws IOException
  {
    assert words != null;
    if (firstEmptyWord < words.length) {
      words = Arrays.copyOf(words, firstEmptyWord);
    }
    s.defaultWriteObject();
  }

  /**
   * Reconstruct the {@link ConciseSet} instance from a stream
   */
  private void readObject(ObjectInputStream s) throws IOException, ClassNotFoundException
  {
    s.defaultReadObject();
    firstEmptyWord = words.length;
    size = -1;
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

    // elements
    f.format("Elements: %s\n", toString());

    // raw representation of words
    for (int i = 0; i < firstEmptyWord; i++) {
      f.format(
          "words[%d] = %s (from %d to %d)\n",
          Integer.valueOf(i),
          toBinaryString(words[i]),
          Integer.valueOf(multiplyByWordSize(i)),
          Integer.valueOf(multiplyByWordSize(i + 1) - 1)
      );
    }

    // object attributes
    f.format("wordsInUse: %d\n", firstEmptyWord);
    f.format("size: %s\n", (size == -1 ? "invalid" : Integer.toString(size)));
    f.format("words.length: %d\n", words.length);

    // compression
    f.format("bitmap compression: %.2f%%\n", 100D * bitmapCompressionRatio());
    f.format("collection compression: %.2f%%\n", 100D * collectionCompressionRatio());

    return s.toString();
  }

  /**
   * Iterates over bits
   * <p/>
   * This iterator allows for modifications during the iteration, that is it
   * is possible to add/remove elements through {@link #add(int)},
   * {@link #remove(int)}, {@link #addAll(IntSet)}, {@link #removeAll(IntSet)}, {@link #retainAll(IntSet)}, etc.. In this case,
   * {@link IntIterator#next()} returns the first integral greater than the
   * last visited one.
   */
  private class BitIterator implements IntIterator
  {
    private int nextIndex;
    private int nextBit;
    private int last;

    /**
     * identify the first bit
     */
    private BitIterator()
    {
      nextIndex = 0;
      if (isEmpty()) {
        return;
      }

      last = -1; // unused!

      // find the first non-empty word
      while (words[nextIndex] == 0) {
        nextIndex++;
      }

      // find the first set bit
      nextBit = Integer.numberOfTrailingZeros(words[nextIndex]);
    }

    /**
     * find the first set bit after nextIndex + nextBit
     */
    void prepareNext()
    {
      // find the next set bit within the current word
      int w = words[nextIndex];
      while ((++nextBit < WORD_SIZE)) {
        if ((w & (1 << nextBit)) != 0) {
          return;
        }
      }

      // find the first non-empty word
      do {
        if (++nextIndex == firstEmptyWord) {
          return;
        }
      } while ((w = words[nextIndex]) == 0);
      nextBit = Integer.numberOfTrailingZeros(w);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext()
    {
      return nextIndex < firstEmptyWord;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int next()
    {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      last = multiplyByWordSize(nextIndex) + nextBit;
      prepareNext();
      return last;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void skipAllBefore(int element)
    {
      if (element <= 0 || element <= last) {
        return;
      }

      // identify where the element is
      int newNextIndex = wordIndexNoCheck(element);
      int newNextBit = element & (WORD_SIZE - 1);
      if (newNextIndex < nextIndex || (newNextIndex == nextIndex && newNextBit <= nextBit)) {
        return;
      }

      // "element" is the next item to return, unless it does not exist
      nextIndex = newNextIndex;
      if (nextIndex >= firstEmptyWord) {
        return;
      }
      nextBit = newNextBit;
      if ((words[nextIndex] & (1 << nextBit)) == 0) {
        prepareNext();
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void remove()
    {
      FastSet.this.remove(last);
    }

    @Override
    public IntIterator clone()
    {
      BitIterator retVal = new BitIterator();
      retVal.nextIndex = nextIndex;
      retVal.nextBit = nextBit;
      retVal.last = last;
      return retVal;
    }
  }

  /**
   * Iterates over bits in reverse order
   * <p/>
   * This iterator allows for modifications during the iteration, that is it
   * is possible to add/remove elements through {@link #add(int)},
   * {@link #remove(int)}, {@link #addAll(IntSet)}, {@link #removeAll(IntSet)}, {@link #retainAll(IntSet)}, etc.. In this case,
   * {@link IntIterator#next()} returns the first integral greater than the
   * last visited one.
   */
  private class ReverseBitIterator implements IntIterator
  {
    private int nextIndex;
    private int nextBit;
    private int last;

    /**
     * identify the first bit
     */
    private ReverseBitIterator()
    {
      nextIndex = firstEmptyWord - 1;
      if (isEmpty()) {
        return;
      }

      last = Integer.MAX_VALUE; // unused!
      nextBit = WORD_SIZE - Integer.numberOfLeadingZeros(words[nextIndex]) - 1;
    }

    /**
     * find the first set bit after nextIndex + nextBit
     */
    void prepareNext()
    {
      // find the next set bit within the current word
      int w = words[nextIndex];
      while ((--nextBit >= 0)) {
        if ((w & (1 << nextBit)) != 0) {
          return;
        }
      }

      // find the first non-empty word
      do {
        if (--nextIndex == -1) {
          return;
        }
      } while ((w = words[nextIndex]) == 0);
      nextBit = WORD_SIZE - Integer.numberOfLeadingZeros(w) - 1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext()
    {
      return nextIndex >= 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int next()
    {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      last = multiplyByWordSize(nextIndex) + nextBit;
      prepareNext();
      return last;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void skipAllBefore(int element)
    {
      if (element < 0) {
        nextIndex = -1;
        return;
      }
      if (element >= last) {
        return;
      }

      // identify where the element is
      int newNextIndex = wordIndexNoCheck(element);
      int newNextBit = element & (WORD_SIZE - 1);
      if (newNextIndex > nextIndex || (newNextIndex == nextIndex && newNextBit >= nextBit)) {
        return;
      }

      // "element" is the next item to return, unless it does not exist
      nextIndex = newNextIndex;
      nextBit = newNextBit;
      if ((words[nextIndex] & (1 << nextBit)) == 0) {
        prepareNext();
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void remove()
    {
      FastSet.this.remove(last);
    }

    @Override
    public IntIterator clone()
    {
      BitIterator retVal = new BitIterator();
      retVal.nextIndex = nextIndex;
      retVal.nextBit = nextBit;
      retVal.last = last;
      return retVal;
    }
  }
}
