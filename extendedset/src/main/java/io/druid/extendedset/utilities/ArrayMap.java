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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * A {@link Map} backed by an array, where keys are the indices of the array,
 * and values are the elements of the array.
 * <p>
 * Modifications to the map (i.e., through {@link #put(Integer, Object)} and
 * {@link java.util.Map.Entry#setValue(Object)}) are reflected to the original array.
 * However, the map has a fixed length, that is the length of the array.
 *
 * @param <T> the type of elements represented by columns
 *
 * @author Alessandro Colantonio
 * @version $Id$
 */
public class ArrayMap<T> extends AbstractMap<Integer, T> implements java.io.Serializable
{
  /**
   * generated serial ID
   */
  private static final long serialVersionUID = -578029467093308343L;

  /**
   * array backed by this map
   */
  private final T[] array;
  /**
   * first index of the map
   */
  private final int indexShift;
  /**
   * {@link Set} instance to iterate over #array
   */
  private transient Set<Entry<Integer, T>> entrySet;

  /**
   * Initializes the map
   *
   * @param array      array to manipulate
   * @param indexShift first index of the map
   */
  ArrayMap(T[] array, int indexShift)
  {
    this.array = array;
    this.indexShift = indexShift;
    entrySet = null;
  }

  /**
   * Initializes the map
   *
   * @param array array to manipulate
   */
  ArrayMap(T[] array)
  {
    this(array, 0);
  }

  /**
   * Test
   *
   * @param args
   */
  public static void main(String[] args)
  {
    ArrayMap<String> am = new ArrayMap<String>(new String[]{"Three", "Four", "Five"}, 3);
    System.out.println(am);
    am.put(5, "FIVE");
    System.out.println(am);
    System.out.println(am.get(5));
    System.out.println(am.containsKey(2));
    System.out.println(am.containsKey(3));
    System.out.println(am.containsValue("THREE"));
    System.out.println(am.keySet());
    System.out.println(am.values());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Set<Entry<Integer, T>> entrySet()
  {
    if (entrySet == null) {
      // create an entry for each element
      final List<SimpleEntry> entries = new ArrayList<SimpleEntry>(array.length);
      for (int i = 0; i < array.length; i++) {
        entries.add(new SimpleEntry(i));
      }

      // create the Set instance
      entrySet = new AbstractSet<Entry<Integer, T>>()
      {
        @Override
        public Iterator<Entry<Integer, T>> iterator()
        {
          return new Iterator<Entry<Integer, T>>()
          {
            int curr = 0;

            @Override
            public boolean hasNext()
            {
              return curr < entries.size();
            }

            @Override
            public Entry<Integer, T> next()
            {
              if (!hasNext()) {
                throw new NoSuchElementException();
              }
              return entries.get(curr++);
            }

            @Override
            public void remove()
            {
              throw new IllegalArgumentException();
            }
          };
        }

        @Override
        public int size()
        {
          return entries.size();
        }
      };
    }
    return entrySet;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int size()
  {
    return array.length;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean containsKey(Object key)
  {
    int index = (Integer) key - indexShift;
    return (index >= 0) && (index < array.length);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public T get(Object key)
  {
    return array[(Integer) key - indexShift];
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public T put(Integer key, T value)
  {
    int actualIndex = key - indexShift;
    T old = array[actualIndex];
    array[actualIndex] = value;
    return old;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode()
  {
    return Arrays.hashCode(array);
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
    if (!super.equals(obj)) {
      return false;
    }
    if (!(obj instanceof ArrayMap<?>)) {
      return false;
    }
    return Arrays.equals(array, ((ArrayMap<?>) obj).array);
  }

  /**
   * Reconstruct the instance from a stream
   */
  private void readObject(ObjectInputStream s) throws IOException, ClassNotFoundException
  {
    s.defaultReadObject();
    entrySet = null;
  }

  /**
   * Entry of the map
   */
  private class SimpleEntry implements Entry<Integer, T>
  {
    /**
     * index of {@link ArrayMap#array}
     */
    final int actualIndex;

    /**
     * Creates an entry
     *
     * @param index index of {@link ArrayMap#array}
     */
    private SimpleEntry(int index)
    {
      this.actualIndex = index;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Integer getKey()
    {
      return actualIndex + indexShift;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T getValue()
    {
      return array[actualIndex];
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T setValue(T value)
    {
      T old = array[actualIndex];
      array[actualIndex] = value;
      return old;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
      return (actualIndex + indexShift) + "=" + array[actualIndex];
    }
  }
}
