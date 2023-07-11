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

package org.apache.druid.msq.querykit;

import com.google.common.base.Preconditions;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.function.Supplier;

@NotThreadSafe
public class LazyResourceHolder<T> implements ResourceHolder<T>
{
  private static final Logger log = new Logger(LazyResourceHolder.class);

  private final Supplier<ResourceHolder<T>> supplier;
  private ResourceHolder<T> supplied = null;

  public LazyResourceHolder(final Supplier<ResourceHolder<T>> supplier)
  {
    this.supplier = Preconditions.checkNotNull(supplier, "supplier");
  }

  @Override
  public T get()
  {
    if (supplied == null) {
      supplied = supplier.get();
    }

    return supplied.get();
  }

  @Override
  public void close()
  {
    if (supplied != null) {
      try {
        supplied.close();
      }
      catch (Throwable e) {
        log.noStackTrace().warn(e, "Exception encountered while closing resource: %s", supplied.get());
      }
      finally {
        supplied = null;
      }
    }
  }
}
