/*
* Licensed to Metamarkets Group Inc. (Metamarkets) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. Metamarkets licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
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
