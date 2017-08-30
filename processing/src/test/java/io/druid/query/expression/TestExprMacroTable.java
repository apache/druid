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

package io.druid.query.expression;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.math.expr.ExprMacroTable;
import io.druid.query.extraction.MapLookupExtractor;
import io.druid.query.lookup.LookupExtractor;
import io.druid.query.lookup.LookupExtractorFactory;
import io.druid.query.lookup.LookupExtractorFactoryContainer;
import io.druid.query.lookup.LookupIntrospectHandler;
import io.druid.query.lookup.LookupReferencesManager;
import org.easymock.EasyMock;

import javax.annotation.Nullable;

public class TestExprMacroTable extends ExprMacroTable
{
  public static final ExprMacroTable INSTANCE = new TestExprMacroTable();

  private TestExprMacroTable()
  {
    super(
        ImmutableList.of(
            new LikeExprMacro(),
            new LookupExprMacro(createTestLookupReferencesManager(ImmutableMap.of("foo", "xfoo"))),
            new RegexpExtractExprMacro(),
            new TimestampCeilExprMacro(),
            new TimestampExtractExprMacro(),
            new TimestampFloorExprMacro(),
            new TimestampFormatExprMacro(),
            new TimestampParseExprMacro(),
            new TimestampShiftExprMacro()
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
