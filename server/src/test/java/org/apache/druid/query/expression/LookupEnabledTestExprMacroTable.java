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

package org.apache.druid.query.expression;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.query.lookup.LookupExtractorFactory;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainer;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.query.lookup.LookupIntrospectHandler;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Separate from {@link TestExprMacroTable} since that one is in druid-processing, which doesn't have
 * {@link LookupExtractorFactoryContainerProvider}.
 */
public class LookupEnabledTestExprMacroTable extends ExprMacroTable
{
  public static final ExprMacroTable INSTANCE = new LookupEnabledTestExprMacroTable();
  public static final String LOOKYLOO = "lookyloo";

  private LookupEnabledTestExprMacroTable()
  {
    super(makeTestMacros(ImmutableMap.of("foo", "xfoo")));
  }

  public static List<ExprMacro> makeTestMacros(final Map<String, String> theLookup)
  {
    return Lists.newArrayList(
        Iterables.concat(
            TestExprMacroTable.INSTANCE.getMacros(),
            Collections.singletonList(
                new LookupExprMacro(
                    createTestLookupProvider(
                        ImmutableMap.of(
                            LOOKYLOO,
                            new MapLookupExtractor(theLookup, false)
                        )
                    )
                )
            )
        )
    );
  }

  /**
   * Returns a {@link LookupExtractorFactoryContainerProvider} that has one lookup, "lookyloo". Public so other tests
   * can use this helper method directly.
   */
  public static LookupExtractorFactoryContainerProvider createTestLookupProvider(final Map<String, LookupExtractor> lookups)
  {
    return new LookupExtractorFactoryContainerProvider()
    {
      @Override
      public Set<String> getAllLookupNames()
      {
        return ImmutableSet.of(LOOKYLOO);
      }

      @Override
      public Optional<LookupExtractorFactoryContainer> get(String lookupName)
      {
        final LookupExtractor theLookup = lookups.get(lookupName);
        if (theLookup != null) {
          return Optional.of(new TestLookupContainer(theLookup));
        } else {
          return Optional.empty();
        }
      }
    };
  }

  public static class TestLookupContainer extends LookupExtractorFactoryContainer
  {
    public TestLookupContainer(final LookupExtractor theLookup)
    {
      super(
          "v0",
          new LookupExtractorFactory()
          {
            @Override
            public boolean start()
            {
              throw new UnsupportedOperationException();
            }

            @Override
            public boolean close()
            {
              throw new UnsupportedOperationException();
            }

            @Override
            public boolean replaces(@Nullable final LookupExtractorFactory other)
            {
              throw new UnsupportedOperationException();
            }

            @Override
            public LookupIntrospectHandler getIntrospectHandler()
            {
              throw new UnsupportedOperationException();
            }

            @Override
            public void awaitInitialization()
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
              return theLookup;
            }
          }
      );
    }
  }
}
