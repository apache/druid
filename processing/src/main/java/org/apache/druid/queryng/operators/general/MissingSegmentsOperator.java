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

package org.apache.druid.queryng.operators.general;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.Iterators;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.OperatorProfile;
import org.apache.druid.queryng.operators.ResultIterator;

import java.util.Collections;
import java.util.List;

/**
 * Trivial operator which only reports missing segments. Should be replaced
 * by something simpler later on.
 *
 * @see {@link org.apache.druid.query.ReportTimelineMissingSegmentQueryRunner}
 */
public class MissingSegmentsOperator<T> implements Operator<T>
{
  private static final Logger LOG = new Logger(MissingSegmentsOperator.class);

  private final List<SegmentDescriptor> descriptors;
  protected final FragmentContext context;

  public MissingSegmentsOperator(FragmentContext context, SegmentDescriptor descriptor)
  {
    this(context, Collections.singletonList(descriptor));
  }

  public MissingSegmentsOperator(FragmentContext context, List<SegmentDescriptor> descriptors)
  {
    this.context = context;
    this.descriptors = descriptors;
    context.register(this);
  }

  @Override
  public ResultIterator<T> open()
  {
    LOG.debug("Reporting missing segments [%s] for query [%s]", descriptors, context.queryId());
    context.responseContext().add(ResponseContext.Keys.MISSING_SEGMENTS, descriptors);
    return Iterators.emptyIterator();
  }

  @Override
  public void close(boolean cascade)
  {
    context.updateProfile(this, OperatorProfile.silentOperator(this));
  }
}
