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

package io.druid.query.expression;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.math.expr.ExprMacroTable;
import io.druid.query.extraction.MapLookupExtractor;
import io.druid.query.lookup.LookupExtractor;
import io.druid.query.lookup.LookupExtractorFactory;
import io.druid.query.lookup.LookupExtractorFactoryContainer;
import io.druid.query.lookup.LookupIntrospectHandler;
import io.druid.query.lookup.LookupReferencesManager;
import org.easymock.EasyMock;

import javax.annotation.Nullable;
import java.util.Collections;

/**
 * Separate from {@link TestExprMacroTable} since that one is in druid-processing, which doesn't have
 * {@link LookupReferencesManager}.
 */
public class LookupEnabledTestExprMacroTable extends ExprMacroTable
{
  public static final ExprMacroTable INSTANCE = new LookupEnabledTestExprMacroTable();

  private LookupEnabledTestExprMacroTable()
  {
    super(
        Lists.newArrayList(
            Iterables.concat(
                TestExprMacroTable.INSTANCE.getMacros(),
                Collections.singletonList(
                    new LookupExprMacro(createTestLookupReferencesManager(ImmutableMap.of("foo", "xfoo")))
                )
            )
        )
    );
  }

  /**
   * Returns a mock {@link LookupReferencesManager} that has one lookup, "lookyloo".
   */
  public static LookupReferencesManager createTestLookupReferencesManager(final ImmutableMap<String, String> theLookup)
  {
    final LookupReferencesManager lookupReferencesManager = EasyMock.createMock(LookupReferencesManager.class);
    EasyMock.expect(lookupReferencesManager.get(EasyMock.eq("lookyloo"))).andReturn(
        new LookupExtractorFactoryContainer(
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
              public LookupExtractor get()
              {
                return new MapLookupExtractor(theLookup, false);
              }
            }
        )
    ).anyTimes();
    EasyMock.expect(lookupReferencesManager.get(EasyMock.not(EasyMock.eq("lookyloo")))).andReturn(null).anyTimes();
    EasyMock.replay(lookupReferencesManager);
    return lookupReferencesManager;
  }
}
