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

package io.druid.extendedset.wrappers.matrix;

import java.io.Serializable;
import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

/**
 * An class that associates a value to each pair within a  {@link PairSet} instance. It is not as fast as  {@link HashMap} , but requires much less memory.
 *
 * @param < T  >  transaction type
 * @param < I  >  item type
 * @param < V  >  type of the value to associate
 *
 * @author Alessandro Colantonio
 * @version $Id: PairMap.java 153 2011-05-30 16:39:57Z cocciasik $
 * @see PairSet
 */
public class PairMap<T, I, V> extends AbstractMap<Pair<T, I>, V> implements Serializable, Cloneable
{
  /**
   * generated serial ID
   */
  private static final long serialVersionUID = 4699094886888004702L;

  /**
   * all existing keys
   *
   * @uml.property name="keys"
   * @uml.associationEnd
   */
  private final PairSet<T, I> keys;

  /**
   * values related to existing keys, according to the ordering provided by {@link #keys}
   */
  private final ArrayList<V> values;

  /**
   * Creates an empty map
   *
   * @param keys {@link PairSet} instance internally used to store indices. If
   *             not empty, {@link #get(Object)} will return <code>null</code>
   *             for each existing pair if we do not also put a value.
   */
  public PairMap(PairSet<T, I> keys)
  {
    this.keys = keys;
    values = new ArrayList<V>(keys.size());
    for (int i = 0; i < keys.size(); i++) {
      values.add(null);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void clear()
  {
    keys.clear();
    values.clear();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean containsKey(Object key)
  {
    return keys.contains(key);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean containsValue(Object value)
  {
    return values.contains(value);
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public V get(Object key)
  {
    if (key == null || !(key instanceof Pair<?, ?>)) {
      return null;
    }
    int index = keys.indexOf((Pair<T, I>) key);
    if (index < 0) {
      return null;
    }
    return values.get(index);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isEmpty()
  {
    return keys.isEmpty();
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public V put(Pair<T, I> key, V value)
  {
    boolean isNew = keys.add(key);
    int index = keys.indexOf(key);
    Object old = null;
    if (isNew) {
      values.add(index, value);
    } else {
      old = values.set(index, value);
    }
    return (V) old;
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public V remove(Object key)
  {
    if (key == null || !(key instanceof Pair<?, ?>)) {
      return null;
    }
    int index = keys.indexOf((Pair<T, I>) key);
    if (index < 0) {
      return null;
    }
    keys.remove(key);
    return values.remove(index);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int size()
  {
    return keys.size();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PairMap<T, I, V> clone()
  {
    // NOTE: do not use super.clone() since it is 10 times slower!
    PairMap<T, I, V> cloned = new PairMap<T, I, V>(keys.clone());
    cloned.values.clear();
    cloned.values.addAll(values);
    return cloned;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Set<Pair<T, I>> keySet()
  {
    return new AbstractSet<Pair<T, I>>()
    {
      @Override
      public boolean add(Pair<T, I> e)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public void clear()
      {
        PairMap.this.clear();
      }

      @Override
      public boolean contains(Object o)
      {
        return keys.contains(o);
      }

      @Override
      public boolean containsAll(Collection<?> c)
      {
        return keys.containsAll(c);
      }

      @Override
      public boolean isEmpty()
      {
        return keys.isEmpty();
      }

      @Override
      public Iterator<Pair<T, I>> iterator()
      {
        return new Iterator<Pair<T, I>>()
        {
          Iterator<Pair<T, I>> itr = keys.iterator();

          @Override
          public boolean hasNext()
          {
            return itr.hasNext();
          }

          @Override
          public Pair<T, I> next()
          {
            return itr.next();
          }

          @Override
          public void remove()
          {
            throw new UnsupportedOperationException();
          }
        };
      }

      @Override
      public boolean remove(Object o)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public int size()
      {
        return keys.size();
      }
    };
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Collection<V> values()
  {
    return new AbstractCollection<V>()
    {

      @Override
      public boolean add(V e)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public void clear()
      {
        PairMap.this.clear();
      }

      @Override
      public boolean contains(Object o)
      {
        return values.contains(o);
      }

      @Override
      public boolean isEmpty()
      {
        return keys.isEmpty();
      }

      @Override
      public Iterator<V> iterator()
      {
        return new Iterator<V>()
        {
          Iterator<V> itr = values.iterator();

          @Override
          public boolean hasNext()
          {
            return itr.hasNext();
          }

          @Override
          public V next()
          {
            return itr.next();
          }

          @Override
          public void remove()
          {
            throw new UnsupportedOperationException();
          }
        };
      }

      @Override
      public boolean remove(Object o)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public int size()
      {
        return values.size();
      }
    };
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Set<Entry<Pair<T, I>, V>> entrySet()
  {
    return new AbstractSet<Entry<Pair<T, I>, V>>()
    {
      @Override
      public boolean add(Entry<Pair<T, I>, V> e)
      {
        V res = PairMap.this.put(e.getKey(), e.getValue());
        return res != e.getValue();
      }

      @Override
      public void clear()
      {
        PairMap.this.clear();
      }

      @Override
      public boolean contains(Object o)
      {
        return o != null
               && o instanceof Entry<?, ?>
               && PairMap.this.containsKey(((Entry<?, ?>) o).getKey())
               && PairMap.this.containsValue(((Entry<?, ?>) o).getValue());
      }

      @Override
      public boolean isEmpty()
      {
        return keys.isEmpty();
      }

      @Override
      public Iterator<Entry<Pair<T, I>, V>> iterator()
      {
        return new Iterator<Entry<Pair<T, I>, V>>()
        {
          final Iterator<Pair<T, I>> keyItr = keys.iterator();
          int valueIndex = -1;

          @Override
          public boolean hasNext()
          {
            return keyItr.hasNext();
          }

          @Override
          public Entry<Pair<T, I>, V> next()
          {
            final Pair<T, I> key = keyItr.next();
            valueIndex++;

            return new Entry<Pair<T, I>, V>()
            {
              @Override
              public Pair<T, I> getKey()
              {
                return key;
              }

              @Override
              public V getValue()
              {
                return values.get(valueIndex);
              }

              @Override
              public V setValue(V value)
              {
                return values.set(valueIndex, value);
              }

              @Override
              public String toString()
              {
                return "{" + getKey() + "=" + getValue() + "}";
              }
            };
          }

          @Override
          public void remove()
          {
            throw new UnsupportedOperationException();
          }
        };
      }

      @Override
      public boolean remove(Object o)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public int size()
      {
        return keys.size();
      }
    };
  }
}
