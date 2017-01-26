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


package io.druid.extendedset;


import io.druid.extendedset.utilities.random.MersenneTwister;

import java.util.Collection;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Generation of random integer sets
 *
 * @author Alessandro Colantonio
 * @version $Id: RandomNumbers.java 142 2011-02-15 23:12:28Z cocciasik $
 */
public abstract class RandomNumbers
{
  /**
   * pseudo-random number generator
   */
  final private static Random RND = new MersenneTwister();

  /**
   * the smallest integer
   */
  protected final int min;

  /**
   * number of elements within the set
   */
  protected final int cardinality;

  /**
   * cardinality to range (i.e., <code>{@link #max} - {@link #min} + 1</code>) ratio
   */
  protected final double density;

  /**
   * Initializes internal data
   *
   * @param cardinality number of elements of the set (i.e., result of
   *                    {@link Collection#size()} )
   * @param density     cardinality to range ratio
   * @param min         the smallest integer
   */
  private RandomNumbers(int cardinality, double density, int min)
  {
    // parameter check
    if (cardinality < 0) {
      throw new IllegalArgumentException("cardinality < 0: " + cardinality);
    }
    if (density < 0D) {
      throw new IllegalArgumentException("density < 0: " + density);
    }
    if (density > 1D) {
      throw new IllegalArgumentException("density > 1: " + density);
    }

    this.cardinality = cardinality;
    this.density = density;
    this.min = min;
  }

  /**
   * Test
   *
   * @param args
   */
  public static void main(String[] args)
  {
    int size = 100;
    System.out.println(new Uniform(size, 0.1, 0).generate());
    System.out.println(new Uniform(size, 0.9, 0).generate());
    System.out.println(new Zipfian(size, 0.1, 0, 2).generate());
    System.out.println(new Zipfian(size, 0.9, 0, 2).generate());
    System.out.println(new Markovian(size, 0.1, 0).generate());
    System.out.println(new Markovian(size, 0.9, 0).generate());
  }

  /**
   * Next integer, according to the given probability distribution
   *
   * @return next pseudo-random integer
   */
  protected abstract int next();

  /**
   * Generates the integer set of pseudo-random numbers
   *
   * @return the integer set
   */
  public SortedSet<Integer> generate()
  {
    SortedSet<Integer> res = new TreeSet<Integer>();
    while (res.size() < cardinality) {
      res.add(next());
    }
    return res;
  }

  /**
   * Integral numbers with uniform distribution.
   * <p>
   * The maximum number will be <code>(cardinality / density) - 1</code>,
   * while the average gap between two consecutive numbers will be
   * <code>density * cardinality</code>.
   */
  public static class Uniform extends RandomNumbers
  {
    /**
     * the greatest integer
     */
    private final int max;

    /**
     * Initializes internal data
     *
     * @param cardinality number of elements of the set (i.e., result of
     *                    {@link Collection#size()} )
     * @param density     cardinality to range ratio
     * @param min         the smallest integer
     */
    public Uniform(int cardinality, double density, int min)
    {
      super(cardinality, density, min);
      max = min + (int) (Math.round(cardinality / density)) - 1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int next()
    {
      return min + RND.nextInt(max - min + 1);
    }
  }

  /**
   * Integral numbers with Zipfian (power-law) distribution.
   * <p>
   * The maximum number will be <code>(cardinality / density) - 1</code>,
   * while the average gap between two consecutive numbers will be
   * <code>density * cardinality</code>. However, integers will be
   * concentrated around the minimum value.
   */
  public static class Zipfian extends RandomNumbers
  {
    /**
     * the greatest integer
     */
    private final int max;

    /**
     * power-law exponent
     */
    private final int k;

    /**
     * Initializes internal data
     *
     * @param cardinality number of elements of the set (i.e., result of
     *                    {@link Collection#size()} )
     * @param density     cardinality to range ratio
     * @param min         the smallest integer
     * @param k           power-law exponent
     */
    public Zipfian(int cardinality, double density, int min, int k)
    {
      super(cardinality, density, min);
      this.k = k;
      max = min + (int) (Math.round(cardinality / density)) - 1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int next()
    {
      return min + (int) ((max - min + 1) * Math.pow(RND.nextDouble(), k));
    }
  }

  /**
   * Integral numbers with Markovian distribution. The data will present
   * sequences of subsequent integers followed by "gaps". In this case,
   * <code>cardinality</code> indicates the probability of switching from a
   * sequence to a gap, and vice-versa. For example, <code>density = 0</code>
   * means a set made up of one long sequence of numbers, while
   * <code>density = 1</code> means a set made up of all odd (or even)
   * integers.
   */
  public static class Markovian extends RandomNumbers
  {
    private boolean skip = false;
    private int next = min;

    /**
     * @param cardinality number of elements of the set (i.e., result of
     *                    {@link Collection#size()} )
     * @param density     cardinality to range ratio
     * @param min         the smallest integer
     */
    public Markovian(int cardinality, double density, int min)
    {
      super(cardinality, density, min);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int next()
    {
      while (skip ^= RND.nextDouble() < density) {
        next++;
      }
      return min + next++;
    }
  }
}
