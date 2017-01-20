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

/**
 * Hash functions for integers and integer arrays.
 *
 * @author Alessandro Colantonio
 * @version $Id: IntHashCode.java 127 2010-12-21 20:22:12Z cocciasik $
 */
public class IntHashCode
{
  /**
   * Computes a hashcode for an integer
   * <p>
   * Inspired by Thomas Wang's function, described at <a
   * href="http://www.concentric.net/~ttwang/tech/inthash.htm"
   * >http://www.concentric.net/~ttwang/tech/inthash.htm</a>
   *
   * @param key the given integer
   *
   * @return the hashcode
   */
  public static int hashCode(int key)
  {
    key = ~key + (key << 15);
    key ^= key >>> 12;
    key += key << 2;
    key ^= key >>> 4;
    key *= 2057;
    key ^= key >>> 16;
    return key;
  }

  /**
   * Computes the hashcode of an array of integers
   *
   * @param keys the given integer array
   *
   * @return the hashcode
   */
  public static int hashCode(int[] keys)
  {
    return hashCode(keys, keys.length, 0);
  }

  /**
   * Computes the hashcode of an array of integers
   * <p>
   * It is based on MurmurHash3 Algorithm, described at <a
   * href="http://sites.google.com/site/murmurhash/"
   * >http://sites.google.com/site/murmurhash</a>
   *
   * @param keys the given integer array
   * @param len  number of elements to include, that is
   *             <code>len <= keys.length</code>
   * @param seed initial seed
   *
   * @return the hashcode
   */
  public static int hashCode(int[] keys, int len, int seed)
  {
    int h = 0x971e137b ^ seed;
    int c1 = 0x95543787;
    int c2 = 0x2ad7eb25;

    for (int i = 0; i < len; i++) {
      int k = keys[i];
      k *= c1;
      k = (k << 11) | (k >>> 21); // rotl k, 11
      k *= c2;
      h ^= k;

      h = (h << 2) - h + 0x52dce729;
      c1 = (c1 << 2) + c1 + 0x7b7d159c;
      c2 = (c2 << 2) + c2 + 0x6bce6396;
    }

    h ^= len;
    h ^= h >>> 16;
    h *= 0x85ebca6b;
    h ^= h >>> 13;
    h *= 0xc2b2ae35;
    h ^= h >>> 16;
    return h;
  }
}
