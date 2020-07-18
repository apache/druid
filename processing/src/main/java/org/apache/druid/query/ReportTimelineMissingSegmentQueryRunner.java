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

package org.apache.druid.query;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.context.ResponseContext;

import java.util.List;

/**
 */
public class ReportTimelineMissingSegmentQueryRunner<T> implements QueryRunner<T>
{
  private static final Logger LOG = new Logger(ReportTimelineMissingSegmentQueryRunner.class);

  private final List<SegmentDescriptor> descriptors;

  public ReportTimelineMissingSegmentQueryRunner(SegmentDescriptor descriptor)
  {
    this(ImmutableList.of(descriptor));
  }

  public ReportTimelineMissingSegmentQueryRunner(List<SegmentDescriptor> descriptors)
  {
    this.descriptors = descriptors;
  }

  @Override
  public Sequence<T> run(QueryPlus<T> queryPlus, ResponseContext responseContext)
  {
    LOG.debug("Reporting a missing segments[%s] for query[%s]", descriptors, queryPlus.getQuery().getId());
    responseContext.add(ResponseContext.Key.MISSING_SEGMENTS, descriptors);
    return Sequences.empty();
  }
}
