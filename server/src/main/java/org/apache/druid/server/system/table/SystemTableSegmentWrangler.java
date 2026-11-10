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

package org.apache.druid.server.system.table;

import com.google.inject.Inject;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.SystemTableAccess;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentWrangler;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;

/**
 * Turns a {@link SystemTableDataSource} into a {@link SystemTableSegment} holding this server's rows of the table.
 *
 * <p>The datasource must carry the {@link SystemTableAccess} that the server attached when it authorized the query.
 * Without it the query fails, rather than returning rows that nobody authorized.</p>
 */
public class SystemTableSegmentWrangler implements SegmentWrangler
{
  private final Map<String, SystemTableDataProvider> dataProviders;
  private final Map<String, SystemTableDescriptor> descriptors;

  @Inject
  public SystemTableSegmentWrangler(
      final Map<String, SystemTableDataProvider> dataProviders,
      final Map<String, SystemTableDescriptor> descriptors
  )
  {
    this.dataProviders = dataProviders;
    this.descriptors = descriptors;
  }

  @Override
  public Iterable<Segment> getSegmentsForIntervals(final DataSource dataSource, final Iterable<Interval> intervals)
  {
    final SystemTableDataSource systemTable = (SystemTableDataSource) dataSource;
    final SystemTableDataProvider provider = dataProviders.get(systemTable.getTable());
    final SystemTableDescriptor descriptor = descriptors.get(systemTable.getTable());
    if (provider == null || descriptor == null) {
      throw InvalidInput.exception("System table[%s] is not served by this service", systemTable.getTable());
    }
    final SystemTableAccess access = systemTable.getAccess();
    if (access == null) {
      throw DruidException.defensive("System table[%s] was not authorized before execution", systemTable.getTable());
    }
    return List.of(new SystemTableSegment(provider, descriptor.getRowSignature(), access));
  }
}
