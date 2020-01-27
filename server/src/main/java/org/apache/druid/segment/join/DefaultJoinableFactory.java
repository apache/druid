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

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class DefaultJoinableFactory implements JoinableFactory
{
  private final List<JoinableFactory> factories;

  @Inject
  public DefaultJoinableFactory(final InlineJoinableFactory inlineJoinableFactory)
  {
    // Just one right now, but we expect there to be more in the future, and maybe even an extension mechanism.
    this.factories = Collections.singletonList(inlineJoinableFactory);
  }

  @Override
  public Optional<Joinable> build(final DataSource dataSource, final JoinConditionAnalysis condition)
  {
    for (JoinableFactory factory : factories) {
      final Optional<Joinable> maybeJoinable = factory.build(dataSource, condition);
      if (maybeJoinable.isPresent()) {
        return maybeJoinable;
      }
    }

    return Optional.empty();
  }
}
