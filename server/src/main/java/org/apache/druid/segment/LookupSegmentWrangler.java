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

import com.google.inject.Inject;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainer;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.query.lookup.LookupSegment;
import org.apache.druid.segment.join.JoinableFactory;
import org.joda.time.Interval;

import java.util.Collections;
import java.util.Optional;

/**
 * A {@link JoinableFactory} for {@link LookupDataSource}.
 *
 * It is not valid to pass any other DataSource type to the "getSegmentsForIntervals" method.
 */
public class LookupSegmentWrangler implements SegmentWrangler
{
  private final LookupExtractorFactoryContainerProvider lookupProvider;

  @Inject
  public LookupSegmentWrangler(final LookupExtractorFactoryContainerProvider lookupProvider)
  {
    this.lookupProvider = lookupProvider;
  }

  @Override
  public Iterable<Segment> getSegmentsForIntervals(final DataSource dataSource, final Iterable<Interval> intervals)
  {
    final LookupDataSource lookupDataSource = (LookupDataSource) dataSource;

    final Optional<LookupExtractorFactoryContainer> maybeContainer =
        lookupProvider.get(lookupDataSource.getLookupName());

    return maybeContainer.map(
        container ->
            Collections.<Segment>singletonList(
                new LookupSegment(
                    lookupDataSource.getLookupName(),
                    container.getLookupExtractorFactory()
                )
            )
    ).orElse(Collections.emptyList());
  }
}
