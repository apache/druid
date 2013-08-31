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

import com.google.common.base.Supplier;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provider;

/**
 */
public class SupplierProvider<T> implements Provider<T>
{
  private final Key<Supplier<T>> supplierKey;

  private Provider<Supplier<T>> supplierProvider;

  public SupplierProvider(
      Key<Supplier<T>> supplierKey
  )
  {
    this.supplierKey = supplierKey;
  }

  @Inject
  public void configure(Injector injector)
  {
    this.supplierProvider = injector.getProvider(supplierKey);
  }

  @Override
  public T get()
  {
    return supplierProvider.get().get();
  }
}
