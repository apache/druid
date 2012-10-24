package com.metamx.druid.kv;

/**
 * An Integer-indexed random-access collection.
 * Typically wraps an {@link Indexed}.
 *
 * @param <T>
 */
public interface Indexed<T> extends Iterable<T> {
  Class<? extends T> getClazz();
  int size();
  T get(int index);
  int indexOf(T value);
}
