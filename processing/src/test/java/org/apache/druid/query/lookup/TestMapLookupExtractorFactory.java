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

import org.apache.druid.query.extraction.MapLookupExtractor;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Kind of like MapLookupExtractorFactory but with no introspection handler so it can live in druid-processing
 */
public class TestMapLookupExtractorFactory implements LookupExtractorFactory
{
  private final MapLookupExtractor lookupExtractor;

  public TestMapLookupExtractorFactory(
      Map<String, String> map,
      boolean isOneToOne
  )
  {
    this.lookupExtractor = new MapLookupExtractor(map, isOneToOne);
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
    return true;
  }

  @Nullable
  @Override
  public LookupIntrospectHandler getIntrospectHandler()
  {
    return null;
  }

  @Override
  public void awaitInitialization() throws InterruptedException, TimeoutException
  {

  }

  @Override
  public boolean isInitialized()
  {
    return true;
  }

  @Override
  public LookupExtractor get()
  {
    return lookupExtractor;
  }
}
