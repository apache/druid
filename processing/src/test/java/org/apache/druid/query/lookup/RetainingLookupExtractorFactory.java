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

package org.apache.druid.query.lookup;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.function.Supplier;

public class RetainingLookupExtractorFactory implements LookupExtractorFactory
{
  private final Supplier<LookupExtractor> lookupSupplier;
  private final Supplier<Optional<RetainedLookupExtractor>> retainedLookupSupplier;

  public RetainingLookupExtractorFactory(
      Supplier<LookupExtractor> lookupSupplier,
      Supplier<Optional<RetainedLookupExtractor>> retainedLookupSupplier
  )
  {
    this.lookupSupplier = lookupSupplier;
    this.retainedLookupSupplier = retainedLookupSupplier;
  }

  @Override
  public boolean start()
  {
    return true;
  }

  @Override
  public boolean close()
  {
    return true;
  }

  @Override
  public boolean replaces(@Nullable LookupExtractorFactory other)
  {
    return false;
  }

  @Nullable
  @Override
  public LookupIntrospectHandler getIntrospectHandler()
  {
    return null;
  }

  @Override
  public void awaitInitialization()
  {
    // noop
  }

  @Override
  public boolean isInitialized()
  {
    return true;
  }

  @Override
  public LookupExtractor get()
  {
    return lookupSupplier.get();
  }

  @Override
  public Optional<RetainedLookupExtractor> acquireRetainedLookupExtractor()
  {
    return retainedLookupSupplier.get();
  }
}
