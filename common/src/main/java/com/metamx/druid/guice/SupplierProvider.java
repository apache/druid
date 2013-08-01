package com.metamx.druid.guice;

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
