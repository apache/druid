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

package io.druid.extendedset.utilities;

import java.util.Random;

/**
 * Population count (a.k.a. Hamming distance) of a bitmap represented by an
 * array of <code>int</code>.
 * <p>
 * Derived from <a
 * href="http://dalkescientific.com/writings/diary/popcnt.c">http
 * ://dalkescientific.com/writings/diary/popcnt.c</a>
 *
 * @author Alessandro Colantonio
 * @version $Id: BitCount.java 157 2011-11-14 14:25:15Z cocciasik $
 */
public class BitCount
{
  /**
   * Population count
   *
   * @param buffer array of <code>int</code>
   *
   * @return population count
   */
  public static int count(int[] buffer)
  {
    return count(buffer, buffer.length);
  }

  /**
   * Population count
   * <p>
   * It counts 24 words at a time, then 3 at a time, then 1 at a time
   *
   * @param buffer array of <code>int</code>
   * @param n      number of elements of <code>buffer</code> to count
   *
   * @return population count
   */
  public static int count(int[] buffer, int n)
  {
    final int n1 = n - n % 24;
    final int n2 = n - n % 3;

    int cnt = 0;
    int i;
    for (i = 0; i < n1; i += 24) {
      cnt += merging3(buffer, i);
    }
    for (; i < n2; i += 3) {
      cnt += merging2(buffer, i);
    }
    cnt += popcount_fbsd2(buffer, i, n);
    return cnt;
  }

  // used by count()
  private static int merging3(int[] buffer, int x)
  {
    int cnt1;
    int cnt2;
    int cnt = 0;
    for (int i = x; i < x + 24; i += 3) {
      cnt1 = buffer[i];
      cnt2 = buffer[i + 1];
      final int w = buffer[i + 2];
      cnt1 = cnt1 - ((cnt1 >>> 1) & 0x55555555) + (w & 0x55555555);
      cnt2 = cnt2 - ((cnt2 >>> 1) & 0x55555555) + ((w >>> 1) & 0x55555555);
      cnt1 = (cnt1 & 0x33333333) + ((cnt1 >>> 2) & 0x33333333);
      cnt1 += (cnt2 & 0x33333333) + ((cnt2 >>> 2) & 0x33333333);
      cnt += (cnt1 & 0x0F0F0F0F) + ((cnt1 >>> 4) & 0x0F0F0F0F);
    }
    cnt = (cnt & 0x00FF00FF) + ((cnt >>> 8) & 0x00FF00FF);
    cnt += cnt >>> 16;
    return cnt & 0x00000FFFF;
  }

  // used by count()
  private static int merging2(int[] buffer, int x)
  {
    int cnt1 = buffer[x];
    int cnt2 = buffer[x + 1];
    final int w = buffer[x + 2];
    cnt1 = cnt1 - ((cnt1 >>> 1) & 0x55555555) + (w & 0x55555555);
    cnt2 = cnt2 - ((cnt2 >>> 1) & 0x55555555) + ((w >>> 1) & 0x55555555);
    cnt1 = (cnt1 & 0x33333333) + ((cnt1 >>> 2) & 0x33333333);
    cnt2 = (cnt2 & 0x33333333) + ((cnt2 >>> 2) & 0x33333333);
    cnt1 += cnt2;
    cnt1 = (cnt1 & 0x0F0F0F0F) + ((cnt1 >>> 4) & 0x0F0F0F0F);
    cnt1 += cnt1 >>> 8;
    cnt1 += cnt1 >>> 16;
    return cnt1 & 0x000000FF;
  }

  // used by count()
  private static int popcount_fbsd2(int[] data, int x, int n)
  {
    int cnt = 0;
    for (; x < n; x++) {
      cnt += Integer.bitCount(data[x]);
    }
    return cnt;
  }

  /**
   * Population count, skipping words at even positions
   *
   * @param buffer array of <code>int</code>
   *
   * @return population count
   */
  public static int count_2(int[] buffer)
  {
    return count_2(buffer, buffer.length);
  }

  /**
   * Population count, skipping words at even positions
   * <p>
   * It counts 24 words at a time, then 3 at a time, then 1 at a time
   *
   * @param buffer array of <code>int</code>
   * @param n      number of elements of <code>buffer</code> to count
   *
   * @return population count
   */
  public static int count_2(int[] buffer, int n)
  {
    final int n1 = n - n % 48;
    final int n2 = n - n % 6;

    int cnt = 0;
    int i;
    for (i = 0; i < n1; i += 48) {
      cnt += merging3_2(buffer, i);
    }
    for (; i < n2; i += 6) {
      cnt += merging2_2(buffer, i);
    }
    cnt += popcount_fbsd2_2(buffer, i, n);
    return cnt;
  }

  // used by count_2()
  private static int merging3_2(int[] buffer, int x)
  {
    int cnt1;
    int cnt2;
    int cnt = 0;
    for (int i = x; i < x + 48; i += 6) {
      cnt1 = buffer[i + 1];
      cnt2 = buffer[i + 3];
      final int w = buffer[i + 5];
      cnt1 = cnt1 - ((cnt1 >>> 1) & 0x55555555) + (w & 0x55555555);
      cnt2 = cnt2 - ((cnt2 >>> 1) & 0x55555555) + ((w >>> 1) & 0x55555555);
      cnt1 = (cnt1 & 0x33333333) + ((cnt1 >>> 2) & 0x33333333);
      cnt1 += (cnt2 & 0x33333333) + ((cnt2 >>> 2) & 0x33333333);
      cnt += (cnt1 & 0x0F0F0F0F) + ((cnt1 >>> 4) & 0x0F0F0F0F);
    }
    cnt = (cnt & 0x00FF00FF) + ((cnt >>> 8) & 0x00FF00FF);
    cnt += cnt >>> 16;
    return cnt & 0x00000FFFF;
  }

  // used by count_2()
  private static int merging2_2(int[] buffer, int x)
  {
    int cnt1 = buffer[x + 1];
    int cnt2 = buffer[x + 3];
    final int w = buffer[x + 5];
    cnt1 = cnt1 - ((cnt1 >>> 1) & 0x55555555) + (w & 0x55555555);
    cnt2 = cnt2 - ((cnt2 >>> 1) & 0x55555555) + ((w >>> 1) & 0x55555555);
    cnt1 = (cnt1 & 0x33333333) + ((cnt1 >>> 2) & 0x33333333);
    cnt2 = (cnt2 & 0x33333333) + ((cnt2 >>> 2) & 0x33333333);
    cnt1 += cnt2;
    cnt1 = (cnt1 & 0x0F0F0F0F) + ((cnt1 >>> 4) & 0x0F0F0F0F);
    cnt1 += cnt1 >>> 8;
    cnt1 += cnt1 >>> 16;
    return cnt1 & 0x000000FF;
  }

  // used by count_2()
  private static int popcount_fbsd2_2(int[] data, int x, int n)
  {
    int cnt = 0;
    for (x++; x < n; x += 2) {
      cnt += Integer.bitCount(data[x]);
    }
    return cnt;
  }

  /**
   * Test
   *
   * @param args
   */
  public static void main(String[] args)
  {
    final int trials = 10000;
    final int maxLength = 10000;

    Random rnd = new Random();
    final int seed = rnd.nextInt();

    System.out.print("Test correctness... ");
    rnd = new Random(seed);
    for (int i = 0; i < trials; i++) {
      int[] x = new int[rnd.nextInt(maxLength)];
      for (int j = 0; j < x.length; j++) {
        x[j] = rnd.nextInt(Integer.MAX_VALUE);
      }

      int size1 = 0;
      for (int j = 0; j < x.length; j++) {
        size1 += Integer.bitCount(x[j]);
      }
      int size2 = count(x);

      if (size1 != size2) {
        System.out.println("i = " + i);
        System.out.println("ERRORE!");
        System.out.println(size1 + ", " + size2);
        for (int j = 0; j < x.length; j++) {
          System.out.format("x[%d] = %d --> %d\n", j, x[j], Integer.bitCount(x[j]));
        }
        return;
      }
    }
    System.out.println("done!");

    System.out.print("Test correctness II... ");
    rnd = new Random(seed);
    for (int i = 0; i < trials; i++) {
      int[] x = new int[rnd.nextInt(maxLength << 1)];
      for (int j = 1; j < x.length; j += 2) {
        x[j] = rnd.nextInt(Integer.MAX_VALUE);
      }

      int size1 = 0;
      for (int j = 1; j < x.length; j += 2) {
        size1 += Integer.bitCount(x[j]);
      }
      int size2 = count_2(x);

      if (size1 != size2) {
        System.out.println("i = " + i);
        System.out.println("ERRORE!");
        System.out.println(size1 + ", " + size2);
        for (int j = 1; j < x.length; j += 2) {
          System.out.format("x[%d] = %d --> %d\n", j, x[j], Integer.bitCount(x[j]));
        }
        return;
      }
    }
    System.out.println("done!");

    System.out.print("Test time count(): ");
    rnd = new Random(seed);
    long t = System.currentTimeMillis();
    for (int i = 0; i < trials; i++) {
      int[] x = new int[rnd.nextInt(maxLength)];
      for (int j = 0; j < x.length; j++) {
        x[j] = rnd.nextInt(Integer.MAX_VALUE);
      }

      @SuppressWarnings("unused")
      int size = 0;
      for (int j = 0; j < x.length; j++) {
        size += Integer.bitCount(x[j]);
      }
    }
    System.out.println(System.currentTimeMillis() - t);

    System.out.print("Test time BitCount.count():   ");
    rnd = new Random(seed);
    t = System.currentTimeMillis();
    for (int i = 0; i < trials; i++) {
      int[] x = new int[rnd.nextInt(maxLength)];
      for (int j = 0; j < x.length; j++) {
        x[j] = rnd.nextInt(Integer.MAX_VALUE);
      }
      count(x);
    }
    System.out.println(System.currentTimeMillis() - t);

    System.out.print("Test II time count(): ");
    rnd = new Random(seed);
    t = System.currentTimeMillis();
    for (int i = 0; i < trials; i++) {
      int[] x = new int[rnd.nextInt(maxLength << 1)];
      for (int j = 1; j < x.length; j += 2) {
        x[j] = rnd.nextInt(Integer.MAX_VALUE);
      }

      @SuppressWarnings("unused")
      int size = 0;
      for (int j = 1; j < x.length; j += 2) {
        size += Integer.bitCount(x[j]);
      }
    }
    System.out.println(System.currentTimeMillis() - t);

    System.out.print("Test II time BitCount.count():   ");
    rnd = new Random(seed);
    t = System.currentTimeMillis();
    for (int i = 0; i < trials; i++) {
      int[] x = new int[rnd.nextInt(maxLength << 1)];
      for (int j = 1; j < x.length; j += 2) {
        x[j] = rnd.nextInt(Integer.MAX_VALUE);
      }
      count_2(x);
    }
    System.out.println(System.currentTimeMillis() - t);
  }
}
