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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LookupSegmentWranglerTest
{

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

        @Override
        public String getCanonicalLookupName(String lookupName)
        {
          return lookupName;
        }
      }
  );

  @Test
  public void test_getSegmentsForIntervals_nonLookup()
  {
    Throwable exception = org.junit.jupiter.api.Assertions.assertThrows(ClassCastException.class, () -> {

      final Iterable<Segment> ignored = factory.getSegmentsForIntervals(
          new TableDataSource("foo"),
          Intervals.ONLY_ETERNITY
      );
    });
    assertTrue(exception.getMessage().contains("TableDataSource cannot be cast"));
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

    Assertions.assertEquals(1, segments.size());
    assertThat(Iterables.getOnlyElement(segments)).isInstanceOf(LookupSegment.class);
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

    Assertions.assertEquals(0, segments.size());
  }
}
