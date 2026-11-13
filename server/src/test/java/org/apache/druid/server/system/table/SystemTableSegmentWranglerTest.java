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

import org.apache.druid.discovery.NodeRole;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.SystemTableAccess;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.column.RowSignature;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class SystemTableSegmentWranglerTest
{
  /**
   * A system table that wasn't authorized on this service can't be read.
   */
  @Test
  public void testRequiresAccess()
  {
    final SystemTableSegmentWrangler wrangler = wrangler();
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> wrangler.getSegmentsForIntervals(new SystemTableDataSource("tasks"), List.of())
    );
    Assertions.assertEquals(DruidException.Category.DEFENSIVE, e.getCategory());
  }

  @Test
  public void testRejectsUnservedTable()
  {
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> wrangler().getSegmentsForIntervals(
            new SystemTableDataSource("segments").withAccess(SystemTableAccess.ALL_ROWS),
            List.of()
        )
    );
    Assertions.assertEquals("System table[segments] is not served by this service", e.getMessage());
  }

  /**
   * The segment returns only the rows that the attached access allows.
   */
  @Test
  public void testAppliesAccess()
  {
    final SystemTableAccess access = new SystemTableAccess(row -> "allowed".equals(row[0]));
    final Segment segment = wrangler().getSegmentsForIntervals(
        new SystemTableDataSource("tasks").withAccess(access),
        List.of()
    ).iterator().next();

    Assertions.assertEquals(
        List.of("task1"),
        SystemTableCursorFactoryTest.readTaskIds(segment.as(CursorFactory.class), CursorBuildSpec.FULL_SCAN)
    );
  }

  private static SystemTableSegmentWrangler wrangler()
  {
    final SystemTableDescriptor descriptor = new SystemTableDescriptor()
    {
      @Override
      public String getTableName()
      {
        return "tasks";
      }

      @Override
      public Set<NodeRole> getNodeRoles()
      {
        return Set.of();
      }

      @Override
      public RowSignature getRowSignature()
      {
        return SystemTableCursorFactoryTest.SIGNATURE;
      }

      @Override
      public SystemTableRowAuthorizer getRowAuthorizer()
      {
        return (authenticationResult, authorizerMapper) -> SystemTableAccess.ALL_ROWS;
      }
    };
    return new SystemTableSegmentWrangler(
        Map.of("tasks", new SystemTableCursorFactoryTest.RecordingProvider()),
        Map.of("tasks", descriptor)
    );
  }
}
