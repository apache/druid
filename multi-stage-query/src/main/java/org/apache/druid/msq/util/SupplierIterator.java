/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.msq.util;

import com.google.common.base.Preconditions;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

/**
 * An Iterator that returns a single element from a {@link Supplier}.
 */
public class SupplierIterator<T> implements Iterator<T>
{
  private Supplier<T> supplier;

  public SupplierIterator(final Supplier<T> supplier)
  {
    this.supplier = Preconditions.checkNotNull(supplier, "supplier");
  }

  @Override
  public boolean hasNext()
  {
    return supplier != null;
  }

  @Override
  public T next()
  {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    final T thing = supplier.get();
    supplier = null;
    return thing;
  }
}
