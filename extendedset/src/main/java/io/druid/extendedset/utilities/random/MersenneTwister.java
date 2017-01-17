package io.druid.extendedset.utilities.random;


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * <h3>MersenneTwister and MersenneTwisterFast</h3>
 * <p><b>Version 13</b>, based on version MT199937(99/10/29)
 * of the Mersenne Twister algorithm found at
 * <a href="http://www.math.keio.ac.jp/matumoto/emt.html">
 * The Mersenne Twister Home Page</a>, with the initialization
 * improved using the new 2002/1/26 initialization algorithm
 * By Sean Luke, October 2004.
 * <p>
 * <p><b>MersenneTwister</b> is a drop-in subclass replacement
 * for java.util.Random.  It is properly synchronized and
 * can be used in a multithreaded environment.  On modern VMs such
 * as HotSpot, it is approximately 1/3 slower than java.util.Random.
 * <p>
 * <p><b>MersenneTwisterFast</b> is not a subclass of java.util.Random.  It has
 * the same public methods as Random does, however, and it is
 * algorithmically identical to MersenneTwister.  MersenneTwisterFast
 * has hard-code inlined all of its methods directly, and made all of them
 * final (well, the ones of consequence anyway).  Further, these
 * methods are <i>not</i> synchronized, so the same MersenneTwisterFast
 * instance cannot be shared by multiple threads.  But all this helps
 * MersenneTwisterFast achieve well over twice the speed of MersenneTwister.
 * java.util.Random is about 1/3 slower than MersenneTwisterFast.
 * <p>
 * <h3>About the Mersenne Twister</h3>
 * <p>This is a Java version of the C-program for MT19937: Integer version.
 * The MT19937 algorithm was created by Makoto Matsumoto and Takuji Nishimura,
 * who ask: "When you use this, send an email to: matumoto@math.keio.ac.jp
 * with an appropriate reference to your work".  Indicate that this
 * is a translation of their algorithm into Java.
 * <p>
 * <p><b>Reference. </b>
 * Makato Matsumoto and Takuji Nishimura,
 * "Mersenne Twister: A 623-Dimensionally Equidistributed Uniform
 * Pseudo-Random Number Generator",
 * <i>ACM Transactions on Modeling and Computer Simulation,</i>
 * Vol. 8, No. 1, January 1998, pp 3--30.
 * <p>
 * <h3>About this Version</h3>
 * <p>
 * <p><b>Changes Since V12:</b> clone() method added.
 * <p>
 * <p><b>Changes Since V11:</b> stateEquals(...) method added.  MersenneTwisterFast
 * is equal to other MersenneTwisterFasts with identical state; likewise
 * MersenneTwister is equal to other MersenneTwister with identical state.
 * This isn't equals(...) because that requires a contract of immutability
 * to compare by value.
 * <p>
 * <p><b>Changes Since V10:</b> A documentation error suggested that
 * setSeed(int[]) required an int[] array 624 long.  In fact, the array
 * can be any non-zero length.  The new version also checks for this fact.
 * <p>
 * <p><b>Changes Since V9:</b> readState(stream) and writeState(stream)
 * provided.
 * <p>
 * <p><b>Changes Since V8:</b> setSeed(int) was only using the first 28 bits
 * of the seed; it should have been 32 bits.  For small-number seeds the
 * behavior is identical.
 * <p>
 * <p><b>Changes Since V7:</b> A documentation error in MersenneTwisterFast
 * (but not MersenneTwister) stated that nextDouble selects uniformly from
 * the full-open interval [0,1].  It does not.  nextDouble's contract is
 * identical across MersenneTwisterFast, MersenneTwister, and java.util.Random,
 * namely, selection in the half-open interval [0,1).  That is, 1.0 should
 * not be returned.  A similar contract exists in nextFloat.
 * <p>
 * <p><b>Changes Since V6:</b> License has changed from LGPL to BSD.
 * New timing information to compare against
 * java.util.Random.  Recent versions of HotSpot have helped Random increase
 * in speed to the point where it is faster than MersenneTwister but slower
 * than MersenneTwisterFast (which should be the case, as it's a less complex
 * algorithm but is synchronized).
 * <p>
 * <p><b>Changes Since V5:</b> New empty constructor made to work the same
 * as java.util.Random -- namely, it seeds based on the current time in
 * milliseconds.
 * <p>
 * <p><b>Changes Since V4:</b> New initialization algorithms.  See
 * (see <a href="http://www.math.keio.ac.jp/matumoto/MT2002/emt19937ar.html"</a>
 * http://www.math.keio.ac.jp/matumoto/MT2002/emt19937ar.html</a>)
 * <p>
 * <p>The MersenneTwister code is based on standard MT19937 C/C++
 * code by Takuji Nishimura,
 * with suggestions from Topher Cooper and Marc Rieffel, July 1997.
 * The code was originally translated into Java by Michael Lecuyer,
 * January 1999, and the original code is Copyright (c) 1999 by Michael Lecuyer.
 * <p>
 * <h3>Java notes</h3>
 * <p>
 * <p>This implementation implements the bug fixes made
 * in Java 1.2's version of Random, which means it can be used with
 * earlier versions of Java.  See
 * <a href="http://www.javasoft.com/products/jdk/1.2/docs/api/java/util/Random.html">
 * the JDK 1.2 java.util.Random documentation</a> for further documentation
 * on the random-number generation contracts made.  Additionally, there's
 * an undocumented bug in the JDK java.util.Random.nextBytes() method,
 * which this code fixes.
 * <p>
 * <p> Just like java.util.Random, this
 * generator accepts a long seed but doesn't use all of it.  java.util.Random
 * uses 48 bits.  The Mersenne Twister instead uses 32 bits (int size).
 * So it's best if your seed does not exceed the int range.
 * <p>
 * <p>MersenneTwister can be used reliably
 * on JDK version 1.1.5 or above.  Earlier Java versions have serious bugs in
 * java.util.Random; only MersenneTwisterFast (and not MersenneTwister nor
 * java.util.Random) should be used with them.
 * <p>
 * <h3>License</h3>
 * <p>
 * Copyright (c) 2003 by Sean Luke. <br>
 * Portions copyright (c) 1993 by Michael Lecuyer. <br>
 * All rights reserved. <br>
 * <p>
 * <p>Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * <ul>
 * <li> Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 * <li> Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * <li> Neither the name of the copyright owners, their employers, nor the
 * names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
 * </ul>
 * <p>THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * @version 13
 */

@SuppressWarnings("serial")
public class MersenneTwister extends java.util.Random implements Cloneable
{
  // Period parameters
  private static final int N = 624;
  private static final int M = 397;
  private static final int MATRIX_A = 0x9908b0df;   //    private static final * constant vector a
  private static final int UPPER_MASK = 0x80000000; // most significant w-r bits
  private static final int LOWER_MASK = 0x7fffffff; // least significant r bits

  // Tempering parameters
  private static final int TEMPERING_MASK_B = 0x9d2c5680;
  private static final int TEMPERING_MASK_C = 0xefc60000;

  private int mt[]; // the array for the state vector
  private int mti; // mti==N+1 means mt[N] is not initialized
  private int mag01[];

  // a good initial seed (of int size, though stored in a long)
  //private static final long GOOD_SEED = 4357;

    /* implemented here because there's a bug in Random's implementation
       of the Gaussian code (divide by zero, and log(0), ugh!), yet its
       gaussian variables are private so we can't access them here.  :-( */

  private double __nextNextGaussian;
  private boolean __haveNextNextGaussian;

    /* We're overriding all internal data, to my knowledge, so this should be okay */

  /**
   * Constructor using the default seed.
   */
  public MersenneTwister()
  {
    this(System.currentTimeMillis());
  }

  /**
   * Constructor using a given seed.  Though you pass this seed in
   * as a long, it's best to make sure it's actually an integer.
   *
   * @param seed
   */
  public MersenneTwister(final long seed)
  {
    super(seed);    /* just in case */
    setSeed(seed);
  }

  /**
   * Constructor using an array of integers as seed.
   * Your array must have a non-zero length.  Only the first 624 integers
   * in the array are used; if the array is shorter than this then
   * integers are repeatedly used in a wrap-around fashion.
   *
   * @param array
   */
  public MersenneTwister(final int[] array)
  {
    super(System.currentTimeMillis());    /* pick something at random just in case */
    setSeed(array);
  }

  /**
   * Tests the code.
   *
   * @param args
   */
  public static void main(String args[])
  {
    int j;

    MersenneTwister r;

    // CORRECTNESS TEST
    // COMPARE WITH http://www.math.keio.ac.jp/matumoto/CODES/MT2002/mt19937ar.out

    r = new MersenneTwister(new int[]{0x123, 0x234, 0x345, 0x456});
    System.out.println("Output of MersenneTwister with new (2002/1/26) seeding mechanism");
    for (j = 0; j < 1000; j++) {
      // first, convert the int from signed to "unsigned"
      long l = r.nextInt();
      if (l < 0) {
        l += 4294967296L;  // max int value
      }
      String s = String.valueOf(l);
      while (s.length() < 10) {
        s = " " + s;  // buffer
      }
      System.out.print(s + " ");
      if (j % 5 == 4) {
        System.out.println();
      }
    }

    // SPEED TEST

    final long SEED = 4357;

    int xx;
    long ms;
    System.out.println("\nTime to test grabbing 100000000 ints");

    r = new MersenneTwister(SEED);
    ms = System.currentTimeMillis();
    xx = 0;
    for (j = 0; j < 100000000; j++) {
      xx += r.nextInt();
    }
    System.out.println("Mersenne Twister: " + (System.currentTimeMillis() - ms) + "          Ignore this: " + xx);

    System.out.println("To compare this with java.util.Random, run this same test on MersenneTwisterFast.");
    System.out.println("The comparison with Random is removed from MersenneTwister because it is a proper");
    System.out.println("subclass of Random and this unfairly makes some of Random's methods un-inlinable,");
    System.out.println("so it would make Random look worse than it is.");

    // TEST TO COMPARE TYPE CONVERSION BETWEEN
    // MersenneTwisterFast.java AND MersenneTwister.java


    System.out.println("\nGrab the first 1000 booleans");
    r = new MersenneTwister(SEED);
    for (j = 0; j < 1000; j++) {
      System.out.print(r.nextBoolean() + " ");
      if (j % 8 == 7) {
        System.out.println();
      }
    }
    if (!(j % 8 == 7)) {
      System.out.println();
    }

    System.out.println("\nGrab 1000 booleans of increasing probability using nextBoolean(double)");
    r = new MersenneTwister(SEED);
    for (j = 0; j < 1000; j++) {
      System.out.print(r.nextBoolean(j / 999.0) + " ");
      if (j % 8 == 7) {
        System.out.println();
      }
    }
    if (!(j % 8 == 7)) {
      System.out.println();
    }

    System.out.println("\nGrab 1000 booleans of increasing probability using nextBoolean(float)");
    r = new MersenneTwister(SEED);
    for (j = 0; j < 1000; j++) {
      System.out.print(r.nextBoolean(j / 999.0f) + " ");
      if (j % 8 == 7) {
        System.out.println();
      }
    }
    if (!(j % 8 == 7)) {
      System.out.println();
    }

    byte[] bytes = new byte[1000];
    System.out.println("\nGrab the first 1000 bytes using nextBytes");
    r = new MersenneTwister(SEED);
    r.nextBytes(bytes);
    for (j = 0; j < 1000; j++) {
      System.out.print(bytes[j] + " ");
      if (j % 16 == 15) {
        System.out.println();
      }
    }
    if (!(j % 16 == 15)) {
      System.out.println();
    }

    byte b;
    System.out.println("\nGrab the first 1000 bytes -- must be same as nextBytes");
    r = new MersenneTwister(SEED);
    for (j = 0; j < 1000; j++) {
      System.out.print((b = r.nextByte()) + " ");
      if (b != bytes[j]) {
        System.out.print("BAD ");
      }
      if (j % 16 == 15) {
        System.out.println();
      }
    }
    if (!(j % 16 == 15)) {
      System.out.println();
    }

    System.out.println("\nGrab the first 1000 shorts");
    r = new MersenneTwister(SEED);
    for (j = 0; j < 1000; j++) {
      System.out.print(r.nextShort() + " ");
      if (j % 8 == 7) {
        System.out.println();
      }
    }
    if (!(j % 8 == 7)) {
      System.out.println();
    }

    System.out.println("\nGrab the first 1000 ints");
    r = new MersenneTwister(SEED);
    for (j = 0; j < 1000; j++) {
      System.out.print(r.nextInt() + " ");
      if (j % 4 == 3) {
        System.out.println();
      }
    }
    if (!(j % 4 == 3)) {
      System.out.println();
    }

    System.out.println("\nGrab the first 1000 ints of different sizes");
    r = new MersenneTwister(SEED);
    int max = 1;
    for (j = 0; j < 1000; j++) {
      System.out.print(r.nextInt(max) + " ");
      max *= 2;
      if (max <= 0) {
        max = 1;
      }
      if (j % 4 == 3) {
        System.out.println();
      }
    }
    if (!(j % 4 == 3)) {
      System.out.println();
    }

    System.out.println("\nGrab the first 1000 longs");
    r = new MersenneTwister(SEED);
    for (j = 0; j < 1000; j++) {
      System.out.print(r.nextLong() + " ");
      if (j % 3 == 2) {
        System.out.println();
      }
    }
    if (!(j % 3 == 2)) {
      System.out.println();
    }

    System.out.println("\nGrab the first 1000 longs of different sizes");
    r = new MersenneTwister(SEED);
    long max2 = 1;
    for (j = 0; j < 1000; j++) {
      System.out.print(r.nextLong(max2) + " ");
      max2 *= 2;
      if (max2 <= 0) {
        max2 = 1;
      }
      if (j % 4 == 3) {
        System.out.println();
      }
    }
    if (!(j % 4 == 3)) {
      System.out.println();
    }

    System.out.println("\nGrab the first 1000 floats");
    r = new MersenneTwister(SEED);
    for (j = 0; j < 1000; j++) {
      System.out.print(r.nextFloat() + " ");
      if (j % 4 == 3) {
        System.out.println();
      }
    }
    if (!(j % 4 == 3)) {
      System.out.println();
    }

    System.out.println("\nGrab the first 1000 doubles");
    r = new MersenneTwister(SEED);
    for (j = 0; j < 1000; j++) {
      System.out.print(r.nextDouble() + " ");
      if (j % 3 == 2) {
        System.out.println();
      }
    }
    if (!(j % 3 == 2)) {
      System.out.println();
    }

    System.out.println("\nGrab the first 1000 gaussian doubles");
    r = new MersenneTwister(SEED);
    for (j = 0; j < 1000; j++) {
      System.out.print(r.nextGaussian() + " ");
      if (j % 3 == 2) {
        System.out.println();
      }
    }
    if (!(j % 3 == 2)) {
      System.out.println();
    }

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Object clone() throws CloneNotSupportedException
  {
    MersenneTwister f = (MersenneTwister) (super.clone());
    f.mt = mt.clone();
    f.mag01 = mag01.clone();
    return f;
  }

  /**
   * @param o
   *
   * @return ?
   */
  public boolean stateEquals(Object o)
  {
    if (o == this) {
      return true;
    }
    if (o == null || !(o instanceof MersenneTwister)) {
      return false;
    }
    MersenneTwister other = (MersenneTwister) o;
    if (mti != other.mti) {
      return false;
    }
    for (int x = 0; x < mag01.length; x++) {
      if (mag01[x] != other.mag01[x]) {
        return false;
      }
    }
    for (int x = 0; x < mt.length; x++) {
      if (mt[x] != other.mt[x]) {
        return false;
      }
    }
    return true;
  }

  /**
   * Reads the entire state of the MersenneTwister RNG from the stream
   *
   * @param stream
   *
   * @throws IOException
   */
  public void readState(DataInputStream stream) throws IOException
  {
    int len = mt.length;
    for (int x = 0; x < len; x++) {
      mt[x] = stream.readInt();
    }

    len = mag01.length;
    for (int x = 0; x < len; x++) {
      mag01[x] = stream.readInt();
    }

    mti = stream.readInt();
    __nextNextGaussian = stream.readDouble();
    __haveNextNextGaussian = stream.readBoolean();
  }

  /**
   * Writes the entire state of the MersenneTwister RNG to the stream
   *
   * @param stream
   *
   * @throws IOException
   */
  public void writeState(DataOutputStream stream) throws IOException
  {
    int len = mt.length;
    for (int x = 0; x < len; x++) {
      stream.writeInt(mt[x]);
    }

    len = mag01.length;
    for (int x = 0; x < len; x++) {
      stream.writeInt(mag01[x]);
    }

    stream.writeInt(mti);
    stream.writeDouble(__nextNextGaussian);
    stream.writeBoolean(__haveNextNextGaussian);
  }

  /**
   * Initialize the pseudo random number generator.  Don't
   * pass in a long that's bigger than an int (Mersenne Twister
   * only uses the first 32 bits for its seed).
   */
  @Override
  synchronized public void setSeed(final long seed)
  {
    // it's always good style to call super
    super.setSeed(seed);

    // Due to a bug in java.util.Random clear up to 1.2, we're
    // doing our own Gaussian variable.
    __haveNextNextGaussian = false;

    mt = new int[N];

    mag01 = new int[2];
    mag01[0] = 0x0;
    mag01[1] = MATRIX_A;

    mt[0] = (int) (seed & 0xffffffff);
    for (mti = 1; mti < N; mti++) {
      mt[mti] =
          (1812433253 * (mt[mti - 1] ^ (mt[mti - 1] >>> 30)) + mti);
            /* See Knuth TAOCP Vol2. 3rd Ed. P.106 for multiplier. */
            /* In the previous versions, MSBs of the seed affect   */
            /* only MSBs of the array mt[].                        */
            /* 2002/01/09 modified by Makoto Matsumoto             */
      mt[mti] &= 0xffffffff;
            /* for >32 bit machines */
    }
  }

  /**
   * Sets the seed of the MersenneTwister using an array of integers.
   * Your array must have a non-zero length.  Only the first 624 integers
   * in the array are used; if the array is shorter than this then
   * integers are repeatedly used in a wrap-around fashion.
   *
   * @param array
   */
  synchronized public void setSeed(final int[] array)
  {
    if (array.length == 0) {
      throw new IllegalArgumentException("Array length must be greater than zero");
    }
    int i, j, k;
    setSeed(19650218);
    i = 1;
    j = 0;
    k = (N > array.length ? N : array.length);
    for (; k != 0; k--) {
      mt[i] = (mt[i] ^ ((mt[i - 1] ^ (mt[i - 1] >>> 30)) * 1664525)) + array[j] + j; /* non linear */
      mt[i] &= 0xffffffff; /* for WORDSIZE > 32 machines */
      i++;
      j++;
      if (i >= N) {
        mt[0] = mt[N - 1];
        i = 1;
      }
      if (j >= array.length) {
        j = 0;
      }
    }
    for (k = N - 1; k != 0; k--) {
      mt[i] = (mt[i] ^ ((mt[i - 1] ^ (mt[i - 1] >>> 30)) * 1566083941)) - i; /* non linear */
      mt[i] &= 0xffffffff; /* for WORDSIZE > 32 machines */
      i++;
      if (i >= N) {
        mt[0] = mt[N - 1];
        i = 1;
      }
    }
    mt[0] = 0x80000000; /* MSB is 1; assuring non-zero initial array */
  }
    
    /* If you've got a truly old version of Java, you can omit these
       two next methods. */

  /**
   * Returns an integer with <i>bits</i> bits filled with a random number.
   */
  @Override
  synchronized protected int next(final int bits)
  {
    int y;

    if (mti >= N)   // generate N words at one time
    {
      int kk;
      @SuppressWarnings("hiding")
      final int[] mt = this.mt; // locals are slightly faster
      @SuppressWarnings("hiding")
      final int[] mag01 = this.mag01; // locals are slightly faster

      for (kk = 0; kk < N - M; kk++) {
        y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
        mt[kk] = mt[kk + M] ^ (y >>> 1) ^ mag01[y & 0x1];
      }
      for (; kk < N - 1; kk++) {
        y = (mt[kk] & UPPER_MASK) | (mt[kk + 1] & LOWER_MASK);
        mt[kk] = mt[kk + (M - N)] ^ (y >>> 1) ^ mag01[y & 0x1];
      }
      y = (mt[N - 1] & UPPER_MASK) | (mt[0] & LOWER_MASK);
      mt[N - 1] = mt[M - 1] ^ (y >>> 1) ^ mag01[y & 0x1];

      mti = 0;
    }

    y = mt[mti++];
    y ^= y >>> 11;                          // TEMPERING_SHIFT_U(y)
    y ^= (y << 7) & TEMPERING_MASK_B;       // TEMPERING_SHIFT_S(y)
    y ^= (y << 15) & TEMPERING_MASK_C;      // TEMPERING_SHIFT_T(y)
    y ^= (y >>> 18);                        // TEMPERING_SHIFT_L(y)

    return y >>> (32 - bits);    // hope that's right!
  }

  private synchronized void writeObject(final ObjectOutputStream out)
      throws IOException
  {
    // just so we're synchronized.
    out.defaultWriteObject();
  }

  private synchronized void readObject(final ObjectInputStream in)
      throws IOException, ClassNotFoundException
  {
    // just so we're synchronized.
    in.defaultReadObject();
  }

  /**
   * This method is missing from jdk 1.0.x and below.  JDK 1.1
   * includes this for us, but what the heck.
   */
  @Override
  public boolean nextBoolean() {return next(1) != 0;}

  /**
   * This generates a coin flip with a probability <tt>probability</tt>
   * of returning true, else returning false. <tt>probability</tt> must
   * be between 0.0 and 1.0, inclusive.  Not as precise a random real
   * event as nextBoolean(double), but twice as fast. To explicitly
   * use this, remember you may need to cast to float first.
   *
   * @param probability
   *
   * @return ?
   */
  public boolean nextBoolean(final float probability)
  {
    if (probability < 0.0f || probability > 1.0f) {
      throw new IllegalArgumentException("probability must be between 0.0 and 1.0 inclusive.");
    }
    if (probability == 0.0f) {
      return false;            // fix half-open issues
    } else if (probability == 1.0f) {
      return true;        // fix half-open issues
    }
    return nextFloat() < probability;
  }

  /**
   * This generates a coin flip with a probability <tt>probability</tt>
   * of returning true, else returning false. <tt>probability</tt> must
   * be between 0.0 and 1.0, inclusive.
   *
   * @param probability
   *
   * @return ?
   */
  public boolean nextBoolean(final double probability)
  {
    if (probability < 0.0 || probability > 1.0) {
      throw new IllegalArgumentException("probability must be between 0.0 and 1.0 inclusive.");
    }
    if (probability == 0.0) {
      return false;             // fix half-open issues
    } else if (probability == 1.0) {
      return true; // fix half-open issues
    }
    return nextDouble() < probability;
  }

  /**
   * This method is missing from JDK 1.1 and below.  JDK 1.2
   * includes this for us, but what the heck.
   */
  @Override
  public int nextInt(final int n)
  {
    if (n <= 0) {
      throw new IllegalArgumentException("n must be > 0");
    }

    if ((n & -n) == n) {
      return (int) ((n * (long) next(31)) >> 31);
    }

    int bits, val;
    do {
      bits = next(31);
      val = bits % n;
    }
    while (bits - val + (n - 1) < 0);
    return val;
  }

  /**
   * This method is for completness' sake.
   * Returns a long drawn uniformly from 0 to n-1.  Suffice it to say,
   * n must be > 0, or an IllegalArgumentException is raised.
   *
   * @param n
   *
   * @return ?
   */
  public long nextLong(final long n)
  {
    if (n <= 0) {
      throw new IllegalArgumentException("n must be > 0");
    }

    long bits, val;
    do {
      bits = (nextLong() >>> 1);
      val = bits % n;
    }
    while (bits - val + (n - 1) < 0);
    return val;
  }

  /**
   * A bug fix for versions of JDK 1.1 and below.  JDK 1.2 fixes
   * this for us, but what the heck.
   *
   * @return ?
   */
  @Override
  public double nextDouble()
  {
    return (((long) next(26) << 27) + next(27))
           / (double) (1L << 53);
  }

  /**
   * A bug fix for versions of JDK 1.1 and below.  JDK 1.2 fixes
   * this for us, but what the heck.
   */

  @Override
  public float nextFloat()
  {
    return next(24) / ((float) (1 << 24));
  }

  /**
   * A bug fix for all versions of the JDK.  The JDK appears to
   * use all four bytes in an integer as independent byte values!
   * Totally wrong. I've submitted a bug report.
   */

  @Override
  public void nextBytes(final byte[] bytes)
  {
    for (int x = 0; x < bytes.length; x++) {
      bytes[x] = (byte) next(8);
    }
  }

  /**
   * For completeness' sake, though it's not in java.util.Random.
   *
   * @return ?
   */
  public char nextChar()
  {
    // chars are 16-bit UniCode values
    return (char) (next(16));
  }

  /**
   * For completeness' sake, though it's not in java.util.Random.
   *
   * @return ?
   */
  public short nextShort()
  {
    return (short) (next(16));
  }

  /**
   * For completeness' sake, though it's not in java.util.Random.
   *
   * @return ?
   */
  public byte nextByte()
  {
    return (byte) (next(8));
  }
//        }

  /**
   * A bug fix for all JDK code including 1.2.  nextGaussian can theoretically
   * ask for the log of 0 and divide it by 0! See Java bug
   * <a href="http://developer.java.sun.com/developer/bugParade/bugs/4254501.html">
   * http://developer.java.sun.com/developer/bugParade/bugs/4254501.html</a>
   *
   * @return ?
   */
  @Override
  synchronized public double nextGaussian()
  {
    if (__haveNextNextGaussian) {
      __haveNextNextGaussian = false;
      return __nextNextGaussian;
    }
//        else
//            {
    double v1, v2, s;
    do {
      v1 = 2 * nextDouble() - 1; // between -1.0 and 1.0
      v2 = 2 * nextDouble() - 1; // between -1.0 and 1.0
      s = v1 * v1 + v2 * v2;
    } while (s >= 1 || s == 0);
    double multiplier = /*Strict*/Math.sqrt(-2 * /*Strict*/Math.log(s) / s);
    __nextNextGaussian = v2 * multiplier;
    __haveNextNextGaussian = true;
    return v1 * multiplier;
  }

}
