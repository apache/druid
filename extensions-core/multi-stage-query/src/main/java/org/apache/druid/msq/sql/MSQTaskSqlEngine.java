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
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.error.InvalidSqlInput;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.msq.querykit.QueryKitUtils;
import org.apache.druid.msq.util.ArrayIngestMode;
import org.apache.druid.msq.util.DimensionSchemaUtils;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.sql.calcite.parser.DruidSqlIngest;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.DruidTypeSystem;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.run.EngineFeature;
import org.apache.druid.sql.calcite.run.NativeSqlEngine;
import org.apache.druid.sql.calcite.run.QueryMaker;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.run.SqlEngines;
import org.apache.druid.sql.destination.IngestDestination;
import org.apache.druid.sql.destination.TableDestination;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MSQTaskSqlEngine implements SqlEngine
{
  public static final Set<String> SYSTEM_CONTEXT_PARAMETERS =
      ImmutableSet.<String>builder()
                  .addAll(NativeSqlEngine.SYSTEM_CONTEXT_PARAMETERS)
                  .add(QueryKitUtils.CTX_TIME_COLUMN_NAME)
                  .add(DruidSqlIngest.SQL_EXPORT_FILE_FORMAT)
                  .add(MultiStageQueryContext.CTX_IS_REINDEX)
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
  public void validateContext(Map<String, Object> queryContext)
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
  public boolean featureAvailable(EngineFeature feature)
  {
    switch (feature) {
      case ALLOW_BINDABLE_PLAN:
      case ALLOW_BROADCAST_RIGHTY_JOIN:
      case TIMESERIES_QUERY:
      case TOPN_QUERY:
      case TIME_BOUNDARY_QUERY:
      case GROUPING_SETS:
      case WINDOW_FUNCTIONS:
      case ALLOW_TOP_LEVEL_UNION_ALL:
      case GROUPBY_IMPLICITLY_SORTS:
        return false;
      case UNNEST:
      case CAN_SELECT:
      case CAN_INSERT:
      case CAN_REPLACE:
      case READ_EXTERNAL_DATA:
      case WRITE_EXTERNAL_DATA:
      case SCAN_ORDER_BY_NON_TIME:
      case SCAN_NEEDS_SIGNATURE:
        return true;
      default:
        throw SqlEngines.generateUnrecognizedFeatureException(MSQTaskSqlEngine.class.getSimpleName(), feature);
    }
  }

  @Override
  public QueryMaker buildQueryMakerForSelect(
      final RelRoot relRoot,
      final PlannerContext plannerContext
  )
  {
    validateSelect(plannerContext);

    return new MSQTaskQueryMaker(
        null,
        overlordClient,
        plannerContext,
        jsonMapper,
        relRoot.fields
    );
  }

  public OverlordClient overlordClient()
  {
    return overlordClient;
  }

  @Override
  public QueryMaker buildQueryMakerForInsert(
      final IngestDestination destination,
      final RelRoot relRoot,
      final PlannerContext plannerContext
  )
  {
    validateInsert(
        relRoot.rel,
        relRoot.fields,
        destination instanceof TableDestination
        ? plannerContext.getPlannerToolbox()
                        .rootSchema()
                        .getNamedSchema(plannerContext.getPlannerToolbox().druidSchemaName())
                        .getSchema()
                        .getTable(((TableDestination) destination).getTableName())
        : null,
        plannerContext
    );

    return new MSQTaskQueryMaker(
        destination,
        overlordClient,
        plannerContext,
        jsonMapper,
        relRoot.fields
    );
  }

  /**
   * Checks if the SELECT contains {@link DruidSqlInsert#SQL_INSERT_SEGMENT_GRANULARITY} in the context. This is a
   * defensive cheeck because {@link org.apache.druid.sql.calcite.planner.DruidPlanner} should have called the
   * {@link #validateContext}
   */
  private static void validateSelect(final PlannerContext plannerContext)
  {
    if (plannerContext.queryContext().containsKey(DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY)) {
      throw DruidException
          .forPersona(DruidException.Persona.DEVELOPER)
          .ofCategory(DruidException.Category.DEFENSIVE)
          .build(
              "The SELECT query's context contains invalid parameter [%s] which is supposed to be populated "
              + "by Druid for INSERT queries. If the user is seeing this exception, that means there's a bug in Druid "
              + "that is populating the query context with the segment's granularity.",
              DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY
          );
    }
  }

  /**
   * Engine-specific validation that happens after the query is planned.
   */
  private static void validateInsert(
      final RelNode rootRel,
      final List<Pair<Integer, String>> fieldMappings,
      @Nullable Table targetTable,
      final PlannerContext plannerContext
  )
  {
    final int timeColumnIndex = getTimeColumnIndex(fieldMappings);
    final Granularity segmentGranularity = getSegmentGranularity(plannerContext);

    validateNoDuplicateAliases(fieldMappings);
    validateTimeColumnType(rootRel, timeColumnIndex);
    validateTimeColumnExistsIfNeeded(timeColumnIndex, segmentGranularity);
    validateLimitAndOffset(rootRel, Granularities.ALL.equals(segmentGranularity));
    validateTypeChanges(rootRel, fieldMappings, targetTable, plannerContext);
  }

  /**
   * SQL allows multiple output columns with the same name. However, we don't allow this for INSERT or REPLACE
   * queries, because we use these output names to generate columns in segments. They must be unique.
   */
  private static void validateNoDuplicateAliases(final List<Pair<Integer, String>> fieldMappings)
  {
    final Set<String> aliasesSeen = new HashSet<>();

    for (final Pair<Integer, String> field : fieldMappings) {
      if (!aliasesSeen.add(field.right)) {
        throw InvalidSqlInput.exception("Duplicate field in SELECT: [%s]", field.right);
      }
    }
  }

  /**
   * Validate the time field {@link ColumnHolder#TIME_COLUMN_NAME} has type TIMESTAMP.
   *
   * @param rootRel         root rel
   * @param timeColumnIndex index of the time field
   */
  private static void validateTimeColumnType(final RelNode rootRel, final int timeColumnIndex)
  {
    if (timeColumnIndex < 0) {
      return;
    }

    // Validate the __time field has the proper type.
    final SqlTypeName timeType = rootRel.getRowType().getFieldList().get(timeColumnIndex).getType().getSqlTypeName();
    if (timeType != SqlTypeName.TIMESTAMP) {
      throw InvalidSqlInput.exception(
          "Field[%s] was the wrong type[%s], expected TIMESTAMP",
          ColumnHolder.TIME_COLUMN_NAME,
          timeType
      );
    }
  }

  /**
   * Validate that if segmentGranularity is not ALL, then there is also a {@link ColumnHolder#TIME_COLUMN_NAME} field.
   *
   * @param segmentGranularity granularity from {@link #getSegmentGranularity(PlannerContext)}
   * @param timeColumnIndex    index of the time field
   */
  private static void validateTimeColumnExistsIfNeeded(
      final int timeColumnIndex,
      final Granularity segmentGranularity
  )
  {
    final boolean hasSegmentGranularity = !Granularities.ALL.equals(segmentGranularity);

    if (hasSegmentGranularity && timeColumnIndex < 0) {
      throw InvalidInput.exception(
          "The granularity [%s] specified in the PARTITIONED BY clause of the INSERT query is different from ALL. "
          + "Therefore, the query must specify a time column (named __time).",
          segmentGranularity
      );
    }
  }

  /**
   * Validate that the query does not have an inappropriate LIMIT or OFFSET. LIMIT prevents gathering result key
   * statistics, which INSERT execution logic depends on. (In QueryKit, LIMIT disables statistics generation and
   * funnels everything through a single partition.)
   *
   * LIMIT is allowed when segment granularity is ALL, disallowed otherwise. OFFSET is never allowed.
   *
   * @param rootRel root rel
   * @param limitOk whether LIMIT is ok (OFFSET is never ok)
   */
  private static void validateLimitAndOffset(final RelNode rootRel, final boolean limitOk)
  {
    Sort sort = null;

    if (rootRel instanceof Sort) {
      sort = (Sort) rootRel;
    } else if (rootRel instanceof Project) {
      // Look for Project after a Sort, then validate the sort.
      final Project project = (Project) rootRel;
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
      throw InvalidSqlInput.exception(
          "INSERT and REPLACE queries cannot have a LIMIT unless PARTITIONED BY is \"ALL\"."
      );
    }
    if (sort != null && sort.offset != null) {
      // Found an outer OFFSET that is not allowed.
      throw InvalidSqlInput.exception("INSERT and REPLACE queries cannot have an OFFSET.");
    }
  }

  /**
   * Validate that the query does not include any type changes from string to array or vice versa.
   *
   * These type changes tend to cause problems due to mixing of multi-value strings and string arrays. In particular,
   * many queries written in the "classic MVD" style (treating MVDs as if they were regular strings) will fail when
   * MVDs and arrays are mixed. So, we detect them as invalid.
   *
   * @param rootRel        root rel
   * @param fieldMappings  field mappings from {@link #validateInsert(RelNode, List, Table, PlannerContext)}
   * @param targetTable    table we are inserting (or replacing) into, if any
   * @param plannerContext planner context
   */
  private static void validateTypeChanges(
      final RelNode rootRel,
      final List<Pair<Integer, String>> fieldMappings,
      @Nullable final Table targetTable,
      final PlannerContext plannerContext
  )
  {
    if (targetTable == null) {
      return;
    }

    final Set<String> columnsExcludedFromTypeVerification =
        MultiStageQueryContext.getColumnsExcludedFromTypeVerification(plannerContext.queryContext());
    final ArrayIngestMode arrayIngestMode = MultiStageQueryContext.getArrayIngestMode(plannerContext.queryContext());

    for (Pair<Integer, String> fieldMapping : fieldMappings) {
      final int columnIndex = fieldMapping.left;
      final String columnName = fieldMapping.right;
      final RelDataTypeField oldSqlTypeField =
          targetTable.getRowType(DruidTypeSystem.TYPE_FACTORY).getField(columnName, true, false);

      if (!columnsExcludedFromTypeVerification.contains(columnName) && oldSqlTypeField != null) {
        final ColumnType oldDruidType = Calcites.getColumnTypeForRelDataType(oldSqlTypeField.getType());
        final RelDataType newSqlType = rootRel.getRowType().getFieldList().get(columnIndex).getType();
        final ColumnType newDruidType =
            DimensionSchemaUtils.getDimensionType(Calcites.getColumnTypeForRelDataType(newSqlType), arrayIngestMode);

        if (newDruidType.isArray() && oldDruidType.is(ValueType.STRING)
            || (newDruidType.is(ValueType.STRING) && oldDruidType.isArray())) {
          final StringBuilder messageBuilder = new StringBuilder(
              StringUtils.format(
                  "Cannot write into field[%s] using type[%s] and arrayIngestMode[%s], since the existing type is[%s]",
                  columnName,
                  newSqlType,
                  StringUtils.toLowerCase(arrayIngestMode.toString()),
                  oldSqlTypeField.getType()
              )
          );

          if (newDruidType.is(ValueType.STRING)
              && newSqlType.getSqlTypeName() == SqlTypeName.ARRAY
              && arrayIngestMode == ArrayIngestMode.MVD) {
            // Tried to insert a SQL ARRAY, which got turned into a STRING by arrayIngestMode: mvd.
            messageBuilder.append(". Try setting arrayIngestMode to[array] to retain the SQL type[")
                          .append(newSqlType)
                          .append("]");
          } else if (newDruidType.is(ValueType.ARRAY)
                     && oldDruidType.is(ValueType.STRING)
                     && arrayIngestMode == ArrayIngestMode.ARRAY) {
            // Tried to insert a SQL ARRAY, which stayed an ARRAY, but wasn't compatible with existing STRING.
            messageBuilder.append(". Try wrapping this field using ARRAY_TO_MV(...) AS ")
                          .append(CalciteSqlDialect.DEFAULT.quoteIdentifier(columnName));
          } else if (newDruidType.is(ValueType.STRING) && oldDruidType.is(ValueType.ARRAY)) {
            // Tried to insert a SQL VARCHAR, but wasn't compatible with existing ARRAY.
            messageBuilder.append(". Try");
            if (arrayIngestMode == ArrayIngestMode.MVD) {
              messageBuilder.append(" setting arrayIngestMode to[array] and");
            }
            messageBuilder.append(" adjusting your query to make this column an ARRAY instead of VARCHAR");
          }

          messageBuilder.append(". See https://druid.apache.org/docs/latest/querying/arrays#arrayingestmode "
                                + "for more details about this check and how to override it if needed.");

          throw InvalidSqlInput.exception(StringUtils.encodeForFormat(messageBuilder.toString()));
        }
      }
    }
  }

  /**
   * Returns the index of {@link ColumnHolder#TIME_COLUMN_NAME} within a list of field mappings from
   * {@link #validateInsert(RelNode, List, Table, PlannerContext)}.
   *
   * Returns -1 if the list does not contain a time column.
   */
  private static int getTimeColumnIndex(final List<Pair<Integer, String>> fieldMappings)
  {
    for (final Pair<Integer, String> field : fieldMappings) {
      if (field.right.equals(ColumnHolder.TIME_COLUMN_NAME)) {
        return field.left;
      }
    }

    return -1;
  }

  /**
   * Retrieve the segment granularity for a query.
   */
  private static Granularity getSegmentGranularity(final PlannerContext plannerContext)
  {
    try {
      return QueryKitUtils.getSegmentGranularityFromContext(
          plannerContext.getJsonMapper(),
          plannerContext.queryContextMap()
      );
    }
    catch (Exception e) {
      // This is a defensive check as the DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY in the query context is
      // populated by Druid. If the user entered an incorrect granularity, that should have been flagged before reaching
      // here.
      throw DruidException.forPersona(DruidException.Persona.DEVELOPER)
                          .ofCategory(DruidException.Category.DEFENSIVE)
                          .build(
                              e,
                              "[%s] is not a valid value for [%s]",
                              plannerContext.queryContext().get(DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY),
                              DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY
                          );

    }
  }

  private static RelDataType getMSQStructType(RelDataTypeFactory typeFactory)
  {
    return typeFactory.createStructType(
        ImmutableList.of(
            Calcites.createSqlType(typeFactory, SqlTypeName.VARCHAR)
        ),
        TASK_STRUCT_FIELD_NAMES
    );
  }
}
