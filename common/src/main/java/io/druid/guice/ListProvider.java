/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.guice;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provider;

import java.lang.annotation.Annotation;
import java.util.List;

/**
 */
public class ListProvider<T> implements Provider<List<T>>
{
  private final List<Key<? extends T>> itemsToLoad = Lists.newArrayList();
  private Injector injector;

  public ListProvider<T> add(Class<? extends T> clazz)
  {
    return add(Key.get(clazz));
  }

  public ListProvider<T> add(Class<? extends T> clazz, Class<? extends Annotation> annotation)
  {
    return add(Key.get(clazz, annotation));
  }

  public ListProvider<T> add(Class<? extends T> clazz, Annotation annotation)
  {
    return add(Key.get(clazz, annotation));
  }

  public ListProvider<T> add(Key<? extends T> key)
  {
    itemsToLoad.add(key);
    return this;
  }

  @Inject
  private void configure(Injector injector)
  {
    this.injector = injector;
  }

  @Override
  public List<T> get()
  {
    List<T> retVal = Lists.newArrayListWithExpectedSize(itemsToLoad.size());
    for (Key<? extends T> key : itemsToLoad) {
      retVal.add(injector.getInstance(key));
    }
    return retVal;
  }
}
