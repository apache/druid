package io.druid.extendedset.intset;

/**
 */
public class ConciseSetUtils
{
  /**
   * The highest representable integer.
   * <p/>
   * Its value is computed as follows. The number of bits required to
   * represent the longest sequence of 0's or 1's is
   * <tt>ceil(log<sub>2</sub>(({@link Integer#MAX_VALUE} - 31) / 31)) = 27</tt>.
   * Indeed, at least one literal exists, and the other bits may all be 0's or
   * 1's, that is <tt>{@link Integer#MAX_VALUE} - 31</tt>. If we use:
   * <ul>
   * <li> 2 bits for the sequence type;
   * <li> 5 bits to indicate which bit is set;
   * </ul>
   * then <tt>32 - 5 - 2 = 25</tt> is the number of available bits to
   * represent the maximum sequence of 0's and 1's. Thus, the maximal bit that
   * can be set is represented by a number of 0's equals to
   * <tt>31 * (1 << 25)</tt>, followed by a literal with 30 0's and the
   * MSB (31<sup>st</sup> bit) equal to 1
   */
  public final static int MAX_ALLOWED_INTEGER = 31 * (1 << 25) + 30; // 1040187422

  /**
   * The lowest representable integer.
   */
  public final static int MIN_ALLOWED_SET_BIT = 0;

  /**
   * Maximum number of representable bits within a literal
   */
  public final static int MAX_LITERAL_LENGTH = 31;

  /**
   * Literal that represents all bits set to 1 (and MSB = 1)
   */
  public final static int ALL_ONES_LITERAL = 0xFFFFFFFF;

  /**
   * Literal that represents all bits set to 0 (and MSB = 1)
   */
  public final static int ALL_ZEROS_LITERAL = 0x80000000;

  /**
   * All bits set to 1 and MSB = 0
   */
  public final static int ALL_ONES_WITHOUT_MSB = 0x7FFFFFFF;

  /**
   * Sequence bit
   */
  public final static int SEQUENCE_BIT = 0x40000000;

  /**
   * Calculates the modulus division by 31 in a faster way than using <code>n % 31</code>
   * <p/>
   * This method of finding modulus division by an integer that is one less
   * than a power of 2 takes at most <tt>O(lg(32))</tt> time. The number of operations
   * is at most <tt>12 + 9 * ceil(lg(32))</tt>.
   * <p/>
   * See <a
   * href="http://graphics.stanford.edu/~seander/bithacks.html">http://graphics.stanford.edu/~seander/bithacks.html</a>
   *
   * @param n number to divide
   *
   * @return <code>n % 31</code>
   */
  public static int maxLiteralLengthModulus(int n)
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
  public static int maxLiteralLengthMultiplication(int n)
  {
    return n * 31;
  }

  /**
   * Calculates the division by 31
   *
   * @param n number to divide
   *
   * @return <code>n / 31</code>
   */
  public static int maxLiteralLengthDivision(int n)
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
  public static boolean isLiteral(int word)
  {
    // the highest one bit should be 1, which means the word as a number is negative
    return word < 0;
  }

  /**
   * Checks whether a word contains a sequence of 1's
   *
   * @param word word to check
   *
   * @return <code>true</code> if the given word is a sequence of 1's
   */
  public static boolean isOneSequence(int word)
  {
    // "word" must be 01*
    return (word & 0xC0000000) == SEQUENCE_BIT;
  }

  /**
   * Checks whether a word contains a sequence of 0's
   *
   * @param word word to check
   *
   * @return <code>true</code> if the given word is a sequence of 0's
   */
  public static boolean isZeroSequence(int word)
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
   *         but with no (un)set bit
   */
  public static boolean isSequenceWithNoBits(int word)
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
  public static int getSequenceCount(int word)
  {
    // get the 25 LSB bits
    return word & 0x01FFFFFF;
  }

  public static int getSequenceNumWords(int word)
  {
    return getSequenceCount(word) + 1;
  }

  /**
   * Clears the (un)set bit in a sequence
   *
   * @param word word to check
   *
   * @return the sequence corresponding to the given sequence and with no
   *         (un)set bits
   */
  public static int getSequenceWithNoBits(int word)
  {
    // clear 29 to 25 LSB bits
    return (word & 0xC1FFFFFF);
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
   *         significant bit set to 1</i>.
   */
  public static int getLiteral(int word, boolean simulateWAH)
  {
    if (isLiteral(word)) {
      return word;
    }

    if (simulateWAH) {
      return isZeroSequence(word) ? ALL_ZEROS_LITERAL : ALL_ONES_LITERAL;
    }

    // get bits from 30 to 26 and use them to set the corresponding bit
    // NOTE: "1 << (word >>> 25)" and "1 << ((word >>> 25) & 0x0000001F)" are equivalent
    // NOTE: ">>> 1" is required since 00000 represents no bits and 00001 the LSB bit set
    int literal = (1 << (word >>> 25)) >>> 1;
    return isZeroSequence(word)
           ? (ALL_ZEROS_LITERAL | literal)
           : (ALL_ONES_LITERAL & ~literal);
  }

  public static int getLiteralFromZeroSeqFlipBit(int word)
  {
    int flipBit = getFlippedBit(word);
    if (flipBit > -1) {
      return ALL_ZEROS_LITERAL | flipBitAsBinaryString(flipBit);
    }
    return ALL_ZEROS_LITERAL;
  }

  public static int getLiteralFromOneSeqFlipBit(int word)
  {
    int flipBit = getFlippedBit(word);
    if (flipBit > -1) {
      return ALL_ONES_LITERAL ^ flipBitAsBinaryString(flipBit);
    }
    return ALL_ONES_LITERAL;
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
   *         set/unset bit, returns -1.
   */
  public static int getFlippedBit(int word)
  {
    // get bits from 30 to 26
    // NOTE: "-1" is required since 00000 represents no bits and 00001 the LSB bit set
    return ((word >>> 25) & 0x0000001F) - 1;
  }

  public static int flipBitAsBinaryString(int flipBit)
  {
    return 1 << flipBit;
  }

  /**
   * Gets the number of set bits within the literal word
   *
   * @param word literal word
   *
   * @return the number of set bits within the literal word
   */
  public static int getLiteralBitCount(int word)
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
  public static int getLiteralBits(int word)
  {
    return ALL_ONES_WITHOUT_MSB & word;
  }

  public static boolean isAllOnesLiteral(int word)
  {
    return word == -1;
  }

  public static boolean isAllZerosLiteral(int word)
  {
    // Either 0x80000000 ("all zeros literal" as it is) or 0, that is "zero sequence of 1 block" = 31 bits,
    // i. e. semantically equivalent to "all zeros literal".
    return (word & ALL_ONES_WITHOUT_MSB) == 0;
  }

  public static boolean isLiteralWithSingleZeroBit(int word)
  {
    return isLiteral(word) && Integer.bitCount(word) == 31;
  }

  public static boolean isLiteralWithSingleOneBit(int word)
  {
    return isLiteral(word) && (Integer.bitCount(word) == 2);
  }

  public static int clearBitsAfterInLastWord(int lastWord, int lastSetBit)
  {
    return lastWord & (ALL_ZEROS_LITERAL | (0xFFFFFFFF >>> (31 - lastSetBit)));
  }

  public static int onesUntil(int bit)
  {
    return 0x80000000 | ((1<<bit) - 1);
  }
}
