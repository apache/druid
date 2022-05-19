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

package org.apache.druid.segment.join;

import com.google.inject.Inject;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.segment.join.lookup.LookupJoinable;

import java.util.Optional;

/**
 * A {@link JoinableFactory} for {@link LookupDataSource}.
 *
 * It is not valid to pass any other DataSource type to the "build" method.
 */
public class LookupJoinableFactory implements JoinableFactory
{
  private final LookupExtractorFactoryContainerProvider lookupProvider;

  @Inject
  public LookupJoinableFactory(LookupExtractorFactoryContainerProvider lookupProvider)
  {
    this.lookupProvider = lookupProvider;
  }

  @Override
  public boolean isDirectlyJoinable(DataSource dataSource)
  {
    // this should always be true if this is access through MapJoinableFactory, but check just in case...
    return dataSource instanceof LookupDataSource;
  }

  @Override
  public Optional<Joinable> build(final DataSource dataSource, final JoinConditionAnalysis condition)
  {
    final LookupDataSource lookupDataSource = (LookupDataSource) dataSource;

    if (condition.canHashJoin()) {
      final String lookupName = lookupDataSource.getLookupName();
      return lookupProvider.get(lookupName)
                           .map(c -> LookupJoinable.wrap(c.getLookupExtractorFactory().get()));
    } else {
      return Optional.empty();
    }
  }
}
