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

package org.apache.druid.msq.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Pair;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.msq.querykit.QueryKitUtils;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.run.EngineFeature;
import org.apache.druid.sql.calcite.run.NativeSqlEngine;
import org.apache.druid.sql.calcite.run.QueryMaker;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.run.SqlEngines;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MSQTaskSqlEngine implements SqlEngine
{
  public static final Set<String> SYSTEM_CONTEXT_PARAMETERS =
      ImmutableSet.<String>builder()
                  .addAll(NativeSqlEngine.SYSTEM_CONTEXT_PARAMETERS)
                  .add(MultiStageQueryContext.CTX_DESTINATION)
                  .add(QueryKitUtils.CTX_TIME_COLUMN_NAME)
                  .build();

  public static final List<String> TASK_STRUCT_FIELD_NAMES = ImmutableList.of("TASK");
  private static final String NAME = "msq-task";

  private final OverlordClient overlordClient;
  private final ObjectMapper jsonMapper;

  @Inject
  public MSQTaskSqlEngine(
      final OverlordClient overlordClient,
      final ObjectMapper jsonMapper
  )
  {
    this.overlordClient = overlordClient;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public String name()
  {
    return NAME;
  }

  @Override
  public void validateContext(Map<String, Object> queryContext) throws ValidationException
  {
    SqlEngines.validateNoSpecialContextKeys(queryContext, SYSTEM_CONTEXT_PARAMETERS);
  }

  @Override
  public RelDataType resultTypeForSelect(RelDataTypeFactory typeFactory, RelDataType validatedRowType)
  {
    return getMSQStructType(typeFactory);
  }

  @Override
  public RelDataType resultTypeForInsert(RelDataTypeFactory typeFactory, RelDataType validatedRowType)
  {
    return getMSQStructType(typeFactory);
  }

  @Override
  public boolean feature(EngineFeature feature, PlannerContext plannerContext)
  {
    switch (feature) {
      case ALLOW_BINDABLE_PLAN:
      case TIMESERIES_QUERY:
      case TOPN_QUERY:
      case TIME_BOUNDARY_QUERY:
        return false;
      case CAN_SELECT:
      case CAN_INSERT:
      case CAN_REPLACE:
      case READ_EXTERNAL_DATA:
      case SCAN_ORDER_BY_NON_TIME:
      case SCAN_NEEDS_SIGNATURE:
        return true;
      default:
        throw new IAE("Unrecognized feature: %s", feature);
    }
  }

  @Override
  public QueryMaker buildQueryMakerForSelect(
      final RelRoot relRoot,
      final PlannerContext plannerContext
  ) throws ValidationException
  {
    validateSelect(relRoot.fields, plannerContext);

    return new MSQTaskQueryMaker(
        null,
        overlordClient,
        plannerContext,
        jsonMapper,
        relRoot.fields
    );
  }

  @Override
  public QueryMaker buildQueryMakerForInsert(
      final String targetDataSource,
      final RelRoot relRoot,
      final PlannerContext plannerContext
  ) throws ValidationException
  {
    validateInsert(relRoot.rel, relRoot.fields, plannerContext);

    return new MSQTaskQueryMaker(
        targetDataSource,
        overlordClient,
        plannerContext,
        jsonMapper,
        relRoot.fields
    );
  }

  private static void validateSelect(
      final List<Pair<Integer, String>> fieldMappings,
      final PlannerContext plannerContext
  ) throws ValidationException
  {
    validateNoDuplicateAliases(fieldMappings);

    if (plannerContext.queryContext().containsKey(DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY)) {
      throw new ValidationException(
          StringUtils.format("Cannot use \"%s\" without INSERT", DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY)
      );
    }
  }

  private static void validateInsert(
      final RelNode rootRel,
      final List<Pair<Integer, String>> fieldMappings,
      final PlannerContext plannerContext
  ) throws ValidationException
  {
    validateNoDuplicateAliases(fieldMappings);

    // Find the __time field.
    int timeFieldIndex = -1;

    for (final Pair<Integer, String> field : fieldMappings) {
      if (field.right.equals(ColumnHolder.TIME_COLUMN_NAME)) {
        timeFieldIndex = field.left;

        // Validate the __time field has the proper type.
        final SqlTypeName timeType = rootRel.getRowType().getFieldList().get(field.left).getType().getSqlTypeName();
        if (timeType != SqlTypeName.TIMESTAMP) {
          throw new ValidationException(
              StringUtils.format(
                  "Field \"%s\" must be of type TIMESTAMP (was %s)",
                  ColumnHolder.TIME_COLUMN_NAME,
                  timeType
              )
          );
        }
      }
    }

    // Validate that if segmentGranularity is not ALL then there is also a __time field.
    final Granularity segmentGranularity;

    try {
      segmentGranularity = QueryKitUtils.getSegmentGranularityFromContext(
          plannerContext.queryContextMap()
      );
    }
    catch (Exception e) {
      throw new ValidationException(
          StringUtils.format(
              "Invalid segmentGranularity: %s",
              plannerContext.queryContext().get(DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY)
          ),
          e
      );
    }

    final boolean hasSegmentGranularity = !Granularities.ALL.equals(segmentGranularity);

    // Validate that the query does not have an inappropriate LIMIT or OFFSET. LIMIT prevents gathering result key
    // statistics, which INSERT execution logic depends on. (In QueryKit, LIMIT disables statistics generation and
    // funnels everything through a single partition.)
    validateLimitAndOffset(rootRel, !hasSegmentGranularity);

    if (hasSegmentGranularity && timeFieldIndex < 0) {
      throw new ValidationException(
          StringUtils.format(
              "INSERT queries with segment granularity other than \"all\" must have a \"%s\" field.",
              ColumnHolder.TIME_COLUMN_NAME
          )
      );
    }
  }

  /**
   * SQL allows multiple output columns with the same name, but multi-stage queries doesn't.
   */
  private static void validateNoDuplicateAliases(final List<Pair<Integer, String>> fieldMappings)
      throws ValidationException
  {
    final Set<String> aliasesSeen = new HashSet<>();

    for (final Pair<Integer, String> field : fieldMappings) {
      if (!aliasesSeen.add(field.right)) {
        throw new ValidationException("Duplicate field in SELECT: " + field.right);
      }
    }
  }

  private static void validateLimitAndOffset(final RelNode topRel, final boolean limitOk) throws ValidationException
  {
    Sort sort = null;

    if (topRel instanceof Sort) {
      sort = (Sort) topRel;
    } else if (topRel instanceof Project) {
      // Look for Project after a Sort, then validate the sort.
      final Project project = (Project) topRel;
      if (project.isMapping()) {
        final RelNode projectInput = project.getInput();
        if (projectInput instanceof Sort) {
          sort = (Sort) projectInput;
        }
      }
    }

    if (sort != null && sort.fetch != null && !limitOk) {
      // Found an outer LIMIT that is not allowed.
      // The segment generator relies on shuffle statistics to determine segment intervals when PARTITIONED BY is not ALL,
      // and LIMIT/OFFSET prevent shuffle statistics from being generated. This is because they always send everything
      // to a single partition, so there are no shuffle statistics.
      throw new ValidationException(
          "INSERT and REPLACE queries cannot have a LIMIT unless PARTITIONED BY is \"ALL\"."
      );
    }
    if (sort != null && sort.offset != null) {
      // Found an outer OFFSET that is not allowed.
      throw new ValidationException("INSERT and REPLACE queries cannot have an OFFSET.");
    }
  }

  private static RelDataType getMSQStructType(RelDataTypeFactory typeFactory)
  {
    return typeFactory.createStructType(
        ImmutableList.of(Calcites.createSqlType(typeFactory, SqlTypeName.VARCHAR)),
        TASK_STRUCT_FIELD_NAMES
    );
  }
}
