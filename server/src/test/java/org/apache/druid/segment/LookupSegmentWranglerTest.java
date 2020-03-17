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

package org.apache.druid.segment;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainer;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.query.lookup.LookupSegment;
import org.apache.druid.query.lookup.LookupSegmentTest;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public class LookupSegmentWranglerTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final LookupSegmentWrangler factory = new LookupSegmentWrangler(
      new LookupExtractorFactoryContainerProvider()
      {
        @Override
        public Set<String> getAllLookupNames()
        {
          return ImmutableSet.of(LookupSegmentTest.LOOKUP_NAME);
        }

        @Override
        public Optional<LookupExtractorFactoryContainer> get(final String lookupName)
        {
          if (LookupSegmentTest.LOOKUP_NAME.equals(lookupName)) {
            return Optional.of(
                new LookupExtractorFactoryContainer(
                    "v0",
                    LookupSegmentTest.LOOKUP_EXTRACTOR_FACTORY
                )
            );
          } else {
            return Optional.empty();
          }
        }
      }
  );

  @Test
  public void test_getSegmentsForIntervals_nonLookup()
  {
    expectedException.expect(ClassCastException.class);
    expectedException.expectMessage("TableDataSource cannot be cast");

    final Iterable<Segment> ignored = factory.getSegmentsForIntervals(
        new TableDataSource("foo"),
        Intervals.ONLY_ETERNITY
    );
  }

  @Test
  public void test_getSegmentsForIntervals_lookupThatExists()
  {
    final List<Segment> segments = ImmutableList.copyOf(
        factory.getSegmentsForIntervals(
            new LookupDataSource(LookupSegmentTest.LOOKUP_NAME),
            Intervals.ONLY_ETERNITY
        )
    );

    Assert.assertEquals(1, segments.size());
    Assert.assertThat(Iterables.getOnlyElement(segments), CoreMatchers.instanceOf(LookupSegment.class));
  }

  @Test
  public void test_getSegmentsForIntervals_lookupThatDoesNotExist()
  {
    final List<Segment> segments = ImmutableList.copyOf(
        factory.getSegmentsForIntervals(
            new LookupDataSource("nonexistent"),
            Intervals.ONLY_ETERNITY
        )
    );

    Assert.assertEquals(0, segments.size());
  }
}
