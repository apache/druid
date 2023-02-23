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

package org.apache.druid.extendedset.intset;


/**
 * This class provides a skeletal implementation of the {@link IntSet}
 * interface to minimize the effort required to implement this interface.
 *
 * @version $Id: AbstractIntSet.java 156 2011-09-01 00:13:57Z cocciasik $
 */
public abstract class AbstractIntSet implements IntSet
{

  /**
   * {@inheritDoc}
   */
  @Override
  public abstract IntSet clone();

  /**
   * {@inheritDoc}
   */
  @Override
  public abstract IntIterator iterator();

  /**
   * {@inheritDoc}
   */
  @Override
  public abstract IntIterator descendingIterator();

  /**
   * {@inheritDoc}
   */
  @Override
  public abstract int size();

  /**
   * {@inheritDoc}
   */
  @Override
  public abstract boolean isEmpty();

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString()
  {
    IntIterator itr = iterator();
    if (!itr.hasNext()) {
      return "[]";
    }

    StringBuilder sb = new StringBuilder();
    sb.append('[');
    while (true) {
      int e = itr.next();
      sb.append(e);
      if (!itr.hasNext()) {
        return sb.append(']').toString();
      }
      sb.append(", ");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int compareTo(IntSet o)
  {
    IntIterator thisIterator = this.descendingIterator();
    IntIterator otherIterator = o.descendingIterator();
    while (thisIterator.hasNext() && otherIterator.hasNext()) {
      int thisItem = thisIterator.next();
      int otherItem = otherIterator.next();
      if (thisItem < otherItem) {
        return -1;
      }
      if (thisItem > otherItem) {
        return 1;
      }
    }
    return thisIterator.hasNext() ? 1 : (otherIterator.hasNext() ? -1 : 0);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(Object obj)
  {
    // special cases
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof IntSet)) {
      return false;
    }
    if (size() != ((IntSet) obj).size()) {
      return false;
    }

    // compare all the integrals, according to their natural order
    IntIterator itr1 = iterator();
    IntIterator itr2 = ((IntSet) obj).iterator();
    while (itr1.hasNext()) {
      if (itr1.next() != itr2.next()) {
        return false;
      }
    }
    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode()
  {
    if (isEmpty()) {
      return 0;
    }
    int h = 1;
    IntIterator itr = iterator();
    if (!itr.hasNext()) {
      h = (h << 5) - h + itr.next();
    }
    return h;
  }
}
