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

package org.apache.druid.indexing.overlord.task;

import com.google.common.base.Optional;
import org.apache.druid.indexing.overlord.GlobalTaskLockbox;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskQueryTool;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.http.TaskStateLookup;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.metadata.TaskStorageQueryFilter;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.LikeDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.RangeFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.filter.TypedInFilter;
import org.apache.druid.query.operator.WindowOperatorQuery;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.LegacySegmentSpec;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.system.SystemTableNotLeaderException;
import org.apache.druid.server.system.table.SystemTablePushdownFilter;
import org.apache.druid.server.system.table.TaskTableDescriptor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class TasksTableDataProviderTest
{
  @Test
  public void testAdvertisesAllPushdownFiltersForSupportingStorage()
  {
    final TaskQueryTool taskQueryTool = Mockito.mock(TaskQueryTool.class);
    Mockito.when(taskQueryTool.supportsTaskStatusQueryFilter()).thenReturn(true);

    final List<SystemTablePushdownFilter> filters = dataProvider(taskQueryTool).getPushdownFilters();

    Assertions.assertEquals(6, filters.size());
  }

  @Test
  public void testAdvertisesOnlyStatusPushdownForUnsupportedStorage()
  {
    final TaskQueryTool taskQueryTool = Mockito.mock(TaskQueryTool.class);

    Assertions.assertEquals(
        List.of(new SystemTablePushdownFilter("status", null)),
        dataProvider(taskQueryTool).getPushdownFilters()
    );
  }

  @Test
  public void testPassesExtractedFiltersDirectlyToTaskQueryTool()
  {
    final TaskQueryTool taskQueryTool = Mockito.mock(TaskQueryTool.class);
    final TaskMaster taskMaster = Mockito.mock(TaskMaster.class);
    Mockito.when(taskMaster.isHalfOrFullLeader()).thenReturn(true);
    final List<DimFilter> filters = List.of(new SelectorDimFilter("datasource", "native_sys_a", null));
    Mockito.when(
        taskQueryTool.getTaskStatusPlusList(
            Mockito.eq(TaskStateLookup.ALL),
            Mockito.any(TaskStorageQueryFilter.class)
        )
    )
           .thenReturn(Collections.emptyList());
    final TasksTableDataProvider supplier = new TasksTableDataProvider(
        taskQueryTool,
        taskMaster
    );

    supplier.getRows(filters, Mockito.mock(AuthenticationResult.class));

    final ArgumentCaptor<TaskStorageQueryFilter> filtersCaptor = ArgumentCaptor.forClass(TaskStorageQueryFilter.class);
    Mockito.verify(taskQueryTool).getTaskStatusPlusList(Mockito.eq(TaskStateLookup.ALL), filtersCaptor.capture());
    Assertions.assertEquals(filters, filtersCaptor.getValue().getFilters());
  }

  /** A standby Overlord rejects {@code sys.tasks} instead of returning an incorrectly empty table. */
  @Test
  public void testRejectsRowsOnStandbyOverlord()
  {
    final TaskMaster taskMaster = Mockito.mock(TaskMaster.class);
    final TasksTableDataProvider provider = new TasksTableDataProvider(
        Mockito.mock(TaskQueryTool.class),
        taskMaster
    );

    Assertions.assertThrows(
        SystemTableNotLeaderException.class,
        () -> provider.getRows(Collections.emptyList(), Mockito.mock(AuthenticationResult.class))
    );
  }

  /** A leadership loss before the task-runner snapshot fails instead of returning an incorrectly empty table. */
  @Test
  public void testRejectsRowsWhenTaskRunnerDisappearsDuringLeaderTransition()
  {
    final TaskMaster taskMaster = Mockito.mock(TaskMaster.class);
    Mockito.when(taskMaster.isHalfOrFullLeader()).thenReturn(true);
    Mockito.when(taskMaster.getTaskRunner()).thenReturn(Optional.absent());
    final TaskQueryTool taskQueryTool = new TaskQueryTool(
        Mockito.mock(TaskStorage.class),
        Mockito.mock(GlobalTaskLockbox.class),
        taskMaster,
        null
    );
    final TasksTableDataProvider provider = new TasksTableDataProvider(
        taskQueryTool,
        taskMaster
    );

    Assertions.assertThrows(
        IllegalStateException.class,
        () -> provider.getRows(Collections.emptyList(), Mockito.mock(AuthenticationResult.class))
    );
  }

  /** Losing the selected task runner while reading also fails instead of returning a stale or empty table. */
  @Test
  public void testRejectsRowsWhenTaskRunnerDisappearsAfterSnapshot()
  {
    final TaskMaster taskMaster = Mockito.mock(TaskMaster.class);
    final TaskRunner taskRunner = Mockito.mock(TaskRunner.class);
    final TaskStorage taskStorage = Mockito.mock(TaskStorage.class);
    Mockito.when(taskMaster.isHalfOrFullLeader()).thenReturn(true);
    Mockito.when(taskMaster.getTaskRunner()).thenReturn(Optional.of(taskRunner), Optional.absent());
    Mockito.when(taskRunner.getKnownTasks()).thenReturn(Collections.emptyList());
    Mockito.when(
        taskStorage.getTaskStatusPlusListWithFilter(
            Mockito.anyMap(),
            Mockito.any(TaskStorageQueryFilter.class)
        )
    ).thenReturn(Collections.emptyList());
    final TaskQueryTool taskQueryTool = new TaskQueryTool(
        taskStorage,
        Mockito.mock(GlobalTaskLockbox.class),
        taskMaster,
        null
    );
    final TasksTableDataProvider provider = new TasksTableDataProvider(
        taskQueryTool,
        taskMaster
    );

    Assertions.assertThrows(
        IllegalStateException.class,
        () -> provider.getRows(Collections.emptyList(), Mockito.mock(AuthenticationResult.class))
    );
  }

  @Test
  public void testExtractsExactDataSourceFromAndFilter()
  {
    final ScanQuery query = scanQuery(
        new AndDimFilter(
            new SelectorDimFilter("datasource", "native_sys_a", null),
            new LikeDimFilter("task_id", "native_sys_mvp_%", null, null)
        )
    );

    final List<DimFilter> extracted = extract(query);

    Assertions.assertEquals(2, extracted.size());
    Assertions.assertInstanceOf(SelectorDimFilter.class, extracted.get(0));
    Assertions.assertEquals(Set.of("native_sys_a"), new TaskStorageQueryFilter(extracted).getStringValues(extracted.get(0)));
    Assertions.assertEquals("id", ((LikeDimFilter) extracted.get(1)).getDimension());
  }

  @Test
  public void testPushesMultipleDataSourcesFromOrFilter()
  {
    final ScanQuery query = scanQuery(
        new OrDimFilter(
            new SelectorDimFilter("datasource", "native_sys_a", null),
            new SelectorDimFilter("datasource", "native_sys_b", null)
        )
    );

    final List<DimFilter> extracted = extract(query);

    Assertions.assertEquals(1, extracted.size());
    Assertions.assertEquals(
        Set.of("native_sys_a", "native_sys_b"),
        new TaskStorageQueryFilter(extracted).getStringValues(extracted.get(0))
    );
  }

  @Test
  public void testExtractsNativeEqualityFilter()
  {
    final EqualityFilter filter = new EqualityFilter("datasource", ColumnType.STRING, "native_sys_a", null);

    final List<DimFilter> extracted = extract(scanQuery(filter));

    Assertions.assertEquals(List.of(filter), extracted);
    Assertions.assertEquals(Set.of("native_sys_a"), new TaskStorageQueryFilter(extracted).getStringValues(extracted.get(0)));
  }

  @Test
  public void testExtractsDataSourceFromWindowLeafScan()
  {
    final ScanQuery scanQuery = scanQuery(new SelectorDimFilter("datasource", "native_sys_a", null));
    final WindowOperatorQuery windowQuery = new WindowOperatorQuery(
        new QueryDataSource(scanQuery),
        new LegacySegmentSpec(Intervals.ETERNITY),
        Collections.emptyMap(),
        RowSignature.empty(),
        Collections.emptyList(),
        null
    );

    final List<DimFilter> extracted = extract(windowQuery);

    Assertions.assertEquals(1, extracted.size());
    Assertions.assertEquals(Set.of("native_sys_a"), new TaskStorageQueryFilter(extracted).getStringValues(extracted.get(0)));
  }

  @Test
  public void testExtractsAllStorageBackedTaskFilters()
  {
    final ScanQuery query = scanQuery(
        new AndDimFilter(
            new TypedInFilter("task_id", ColumnType.STRING, List.of("task-a", "task-b"), null, null),
            new SelectorDimFilter("group_id", "group-a", null),
            new SelectorDimFilter("type", "noop", null),
            new SelectorDimFilter("datasource", "native_sys_a", null),
            new EqualityFilter("created_time", ColumnType.STRING, "2026-01-02T00:00:00.000Z", null),
            new RangeFilter(
                "created_time",
                ColumnType.STRING,
                "2026-01-01T00:00:00.000Z",
                "2026-02-01T00:00:00.000Z",
                false,
                true,
                null
            ),
            new SelectorDimFilter("status", "SUCCESS", null),
            new LikeDimFilter("error_msg", "%ignored%", null, null)
        )
    );

    final List<DimFilter> extracted = extract(query);
    final TaskStorageQueryFilter pushdownFilters = new TaskStorageQueryFilter(extracted);

    Assertions.assertEquals(7, extracted.size());
    Assertions.assertEquals("id", pushdownFilters.getStringValuesColumn(extracted.get(0)));
    Assertions.assertEquals(Set.of("task-a", "task-b"), pushdownFilters.getStringValues(extracted.get(0)));
    Assertions.assertEquals(Set.of("group-a"), pushdownFilters.getStringValues(extracted.get(1)));
    Assertions.assertEquals(Set.of("noop"), pushdownFilters.getStringValues(extracted.get(2)));
    Assertions.assertEquals(Set.of("native_sys_a"), pushdownFilters.getStringValues(extracted.get(3)));
    Assertions.assertEquals("created_date", pushdownFilters.getStringValuesColumn(extracted.get(4)));
    Assertions.assertEquals("created_date", ((RangeFilter) extracted.get(5)).getColumn());
    Assertions.assertFalse(pushdownFilters.includesActiveTasks());
    Assertions.assertTrue(pushdownFilters.includesCompleteTasks());
  }

  private static List<DimFilter> extract(final Query<?> query)
  {
    final TaskQueryTool taskQueryTool = Mockito.mock(TaskQueryTool.class);
    Mockito.when(taskQueryTool.supportsTaskStatusQueryFilter()).thenReturn(true);
    final TasksTableDataProvider dataSupplier = dataProvider(taskQueryTool);
    return SystemTablePushdownFilter.extract(query, dataSupplier.getPushdownFilters());
  }

  private static TasksTableDataProvider dataProvider(final TaskQueryTool taskQueryTool)
  {
    return new TasksTableDataProvider(taskQueryTool, null);
  }

  private static ScanQuery scanQuery(final DimFilter filter)
  {
    return Druids.newScanQueryBuilder()
                 .dataSource(new SystemTableDataSource(TaskTableDescriptor.TABLE_NAME))
                 .intervals(new LegacySegmentSpec(Intervals.ETERNITY))
                 .filters(filter)
                 .build();
  }
}
