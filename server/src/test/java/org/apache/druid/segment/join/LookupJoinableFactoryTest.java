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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainer;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.query.lookup.MapLookupExtractorFactory;
import org.apache.druid.segment.join.lookup.LookupJoinable;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;

public class LookupJoinableFactoryTest
{
  private static final String PREFIX = "j.";

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final LookupJoinableFactory factory;
  private final LookupDataSource lookupDataSource = new LookupDataSource("country_code_to_name");

  public LookupJoinableFactoryTest()
  {
    try {
      final MapLookupExtractor countryIsoCodeToNameLookup = JoinTestHelper.createCountryIsoCodeToNameLookup();
      this.factory = new LookupJoinableFactory(
          new LookupExtractorFactoryContainerProvider()
          {
            @Override
            public Set<String> getAllLookupNames()
            {
              return ImmutableSet.of(lookupDataSource.getLookupName());
            }

            @Override
            public Optional<LookupExtractorFactoryContainer> get(String lookupName)
            {
              if (lookupDataSource.getLookupName().equals(lookupName)) {
                return Optional.of(
                    new LookupExtractorFactoryContainer(
                        "v0",
                        new MapLookupExtractorFactory(
                            countryIsoCodeToNameLookup.getMap(),
                            false
                        )
                    )
                );
              } else {
                return Optional.empty();
              }
            }
          }
      );
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testBuildNonLookup()
  {
    expectedException.expect(ClassCastException.class);
    expectedException.expectMessage("TableDataSource cannot be cast");

    final Optional<Joinable> ignored = factory.build(new TableDataSource("foo"), makeCondition("x == \"j.k\""));
  }

  @Test
  public void testBuildNonHashJoin()
  {
    Assert.assertEquals(
        Optional.empty(),
        factory.build(lookupDataSource, makeCondition("x > \"j.k\""))
    );
  }

  @Test
  public void testBuildDifferentLookup()
  {
    Assert.assertEquals(
        Optional.empty(),
        factory.build(new LookupDataSource("beep"), makeCondition("x == \"j.k\""))
    );
  }

  @Test
  public void testBuild()
  {
    final Joinable joinable = factory.build(lookupDataSource, makeCondition("x == \"j.k\"")).get();

    Assert.assertThat(joinable, CoreMatchers.instanceOf(LookupJoinable.class));
    Assert.assertEquals(ImmutableList.of("k", "v"), joinable.getAvailableColumns());
    Assert.assertEquals(Joinable.CARDINALITY_UNKNOWN, joinable.getCardinality("k"));
    Assert.assertEquals(Joinable.CARDINALITY_UNKNOWN, joinable.getCardinality("v"));
  }

  @Test
  public void testIsDirectlyJoinable()
  {
    Assert.assertTrue(factory.isDirectlyJoinable(lookupDataSource));
    Assert.assertFalse(factory.isDirectlyJoinable(new TableDataSource("foo")));
  }

  private static JoinConditionAnalysis makeCondition(final String condition)
  {
    return JoinConditionAnalysis.forExpression(condition, PREFIX, ExprMacroTable.nil());
  }
}
