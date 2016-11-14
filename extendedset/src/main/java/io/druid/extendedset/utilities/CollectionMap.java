package io.druid.extendedset.utilities;

import io.druid.extendedset.ExtendedSet;
import io.druid.extendedset.intset.ConciseSet;
import io.druid.extendedset.wrappers.IntegerSet;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;

/**
 * This class implements a {@link Map} from a key of type <code>K</code> to a
 * collection contains instances of <code>I</code>.
 *
 * @param <K> key type
 * @param <I> item type
 * @param <C> {@link Collection} subclass used to collect items
 *
 * @author Alessandro Colantonio
 * @version $Id: CollectionMap.java 152 2011-03-30 11:18:18Z cocciasik $
 */
public class CollectionMap<K, I, C extends Collection<I>> extends LinkedHashMap<K, C>
{
  private static final long serialVersionUID = -2613391212228461025L;

  /**
   * empty collection
   */
  private final C emptySet;

  /**
   * Initializes the map by providing an instance of the empty collection
   *
   * @param emptySet the empty collection
   */
  public CollectionMap(C emptySet)
  {
    this.emptySet = emptySet;
  }

  /**
   * Generates a new {@link CollectionMap} instance. It is an alternative to
   * the constructor {@link #CollectionMap(Collection)} that reduces the code
   * to write.
   *
   * @param <KX>     key type
   * @param <IX>     item type
   * @param <CX>     {@link Collection} subclass used to collect items
   * @param <EX>     empty subset type
   * @param emptySet the empty collection
   *
   * @return the new instance of {@link CollectionMap}
   */
  public static <KX, IX, CX extends Collection<IX>, EX extends CX>
  CollectionMap<KX, IX, CX> newCollectionMap(EX emptySet)
  {
    return new CollectionMap<KX, IX, CX>(emptySet);
  }

  /**
   * Test procedure
   * <p>
   * Expected output:
   * <pre>
   * {}
   * {A=[1]}
   * {A=[1, 2]}
   * {A=[1, 2], B=[3]}
   * {A=[1, 2], B=[3, 4, 5, 6]}
   * true
   * true
   * false
   * {A=[1], B=[3, 4, 5, 6]}
   * {A=[1], B=[3, 4, 5, 6]}
   * {A=[1], B=[6]}
   * </pre>
   *
   * @param args
   */
  public static void main(String[] args)
  {
    CollectionMap<String, Integer, IntegerSet> map = newCollectionMap(new IntegerSet(new ConciseSet()));
    System.out.println(map);

    map.putItem("A", 1);
    System.out.println(map);

    map.putItem("A", 2);
    System.out.println(map);

    map.putItem("B", 3);
    System.out.println(map);

    map.putAllItems("B", Arrays.asList(4, 5, 6));
    System.out.println(map);

    System.out.println(map.containsItem(1));
    System.out.println(map.containsItem(6));
    System.out.println(map.containsItem(7));

    map.removeItem("A", 2);
    System.out.println(map);

    map.removeItem("A", 3);
    System.out.println(map);

    map.removeAllItems("B", Arrays.asList(1, 2, 3, 4, 5));
    System.out.println(map);
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public CollectionMap<K, I, C> clone()
  {
    // result
    CollectionMap<K, I, C> cloned = new CollectionMap<K, I, C>(emptySet);

    // clone all the entries
    cloned.putAll(this);

    // clone all the values
    if (emptySet instanceof Cloneable) {
      for (Entry<K, C> e : cloned.entrySet()) {
        try {
          e.setValue((C) e.getValue().getClass().getMethod("clone").invoke(e.getValue()));
        }
        catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      }
    } else {
      for (Entry<K, C> e : cloned.entrySet()) {
        C copy = cloneEmptySet();
        copy.addAll(e.getValue());
        e.setValue(copy);
      }
    }
    return cloned;
  }

  /**
   * Generates an empty {@link CollectionMap} instance with the same
   * collection type for values
   *
   * @return the empty {@link CollectionMap} instance
   */
  public CollectionMap<K, I, C> empty()
  {
    return new CollectionMap<K, I, C>(emptySet);
  }

  /**
   * Populates the current instance with the data from another map. In
   * particular, it creates the list of keys associated to each value.
   *
   * @param map the input map
   */
  public void mapValueToKeys(Map<I, K> map)
  {
    for (Entry<I, K> e : map.entrySet()) {
      putItem(e.getValue(), e.getKey());
    }
  }

  /**
   * Generates a clone of the empty set
   *
   * @return a clone of the empty set
   */
  @SuppressWarnings("unchecked")
  private C cloneEmptySet()
  {
    try {
      if (emptySet instanceof Cloneable) {
        return (C) emptySet.getClass().getMethod("clone").invoke(emptySet);
      }
      return (C) emptySet.getClass().newInstance();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Checks if there are some collections that contain the given item
   *
   * @param item item to check
   *
   * @return <code>true</code> if the item exists within some collections
   */
  public boolean containsItem(I item)
  {
    for (Entry<K, C> e : entrySet()) {
      if (e.getValue().contains(item)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Adds an item to the collection corresponding to the given key
   *
   * @param key  the key for the identification of the collection
   * @param item item to add
   *
   * @return the updated collection of items for the given key
   */
  public C putItem(K key, I item)
  {
    C items = get(key);
    if (items == null) {
      put(key, items = cloneEmptySet());
    }
    items.add(item);
    return items;
  }

  /**
   * Adds a collection of items to the collection corresponding to the given key
   *
   * @param key the key for the identification of the collection
   * @param c   items to add
   *
   * @return the updated collection of items for the given key
   */
  public C putAllItems(K key, Collection<? extends I> c)
  {
    C items = get(key);
    if (c == null) {
      put(key, items = cloneEmptySet());
    }
    items.addAll(c);
    return items;
  }

  /**
   * Removes the item from the collection corresponding to the given key
   *
   * @param key  the key for the identification of the collection
   * @param item item to remove
   *
   * @return the updated collection of items for the given key
   */
  public C removeItem(K key, I item)
  {
    C items = get(key);
    if (items == null) {
      return null;
    }
    items.remove(item);
    if (items.isEmpty()) {
      remove(key);
    }
    return items;
  }

  /**
   * Removes a collection of items from the collection corresponding to the given key
   *
   * @param key the key for the identification of the collection
   * @param c   items to remove
   *
   * @return the updated collection of items for the given key
   */
  public C removeAllItems(K key, Collection<? extends I> c)
  {
    C items = get(key);
    if (items == null) {
      return null;
    }
    items.removeAll(c);
    if (items.isEmpty()) {
      remove(key);
    }
    return items;
  }

  /**
   * Makes all collections read-only
   */
  @SuppressWarnings("unchecked")
  public void makeAllCollectionsUnmodifiable()
  {
    if (emptySet instanceof ExtendedSet) {
      for (Entry<K, C> e : entrySet()) {
        e.setValue((C) ((ExtendedSet) e.getValue()).unmodifiable());
      }
    } else if (emptySet instanceof List) {
      for (Entry<K, C> e : entrySet()) {
        e.setValue((C) (Collections.unmodifiableList((List<I>) e.getValue())));
      }
    } else if (emptySet instanceof Set) {
      for (Entry<K, C> e : entrySet()) {
        e.setValue((C) (Collections.unmodifiableSet((Set<I>) e.getValue())));
      }
    } else if (emptySet instanceof SortedSet) {
      for (Entry<K, C> e : entrySet()) {
        e.setValue((C) (Collections.unmodifiableSortedSet((SortedSet<I>) e.getValue())));
      }
    } else {
      for (Entry<K, C> e : entrySet()) {
        e.setValue((C) (Collections.unmodifiableCollection(e.getValue())));
      }
    }

  }
}
