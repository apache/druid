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

package org.apache.druid.sql.calcite.planner;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.prepare.BaseDruidSqlValidator;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.IdentifierNamespace;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.druid.catalog.model.facade.DatasourceFacade;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.error.InvalidSqlInput;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.sql.calcite.parser.DruidSqlIngest;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.apache.druid.sql.calcite.parser.DruidSqlParserUtils;
import org.apache.druid.sql.calcite.parser.ExternalDestinationSqlIdentifier;
import org.apache.druid.sql.calcite.run.EngineFeature;
import org.apache.druid.sql.calcite.table.DatasourceTable;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Druid extended SQL validator. (At present, it doesn't actually
 * have any extensions yet, but it will soon.)
 */
class DruidSqlValidator extends BaseDruidSqlValidator
{
  private static final Pattern UNNAMED_COLUMN_PATTERN = Pattern.compile("^EXPR\\$\\d+$", Pattern.CASE_INSENSITIVE);

  // Copied here from MSQE since that extension is not visible here.
  public static final String CTX_ROWS_PER_SEGMENT = "msqRowsPerSegment";

  public interface ValidatorContext
  {
    Map<String, Object> queryContextMap();
    CatalogResolver catalog();
    String druidSchemaName();
    ObjectMapper jsonMapper();
  }

  private final PlannerContext plannerContext;

  protected DruidSqlValidator(
      SqlOperatorTable opTab,
      CalciteCatalogReader catalogReader,
      JavaTypeFactory typeFactory,
      Config validatorConfig,
      PlannerContext plannerContext
  )
  {
    super(opTab, catalogReader, typeFactory, validatorConfig);
    this.plannerContext = plannerContext;
  }

  @Override
  public void validateWindow(SqlNode windowOrId, SqlValidatorScope scope, @Nullable SqlCall call)
  {
    final SqlWindow targetWindow;
    switch (windowOrId.getKind()) {
      case IDENTIFIER:
        targetWindow = getWindowByName((SqlIdentifier) windowOrId, scope);
        break;
      case WINDOW:
        targetWindow = (SqlWindow) windowOrId;
        break;
      default:
        throw Util.unexpected(windowOrId.getKind());
    }


    @Nullable
    SqlNode lowerBound = targetWindow.getLowerBound();
    @Nullable
    SqlNode upperBound = targetWindow.getUpperBound();
    if (!isValidEndpoint(lowerBound) || !isValidEndpoint(upperBound)) {
      throw buildCalciteContextException(
          "Window frames with expression based lower/upper bounds are not supported.",
          windowOrId
      );
    }

    if (isPrecedingOrFollowing(lowerBound) &&
        isPrecedingOrFollowing(upperBound) &&
        lowerBound.getKind() == upperBound.getKind()) {
      // this limitation can be lifted when https://github.com/apache/druid/issues/15739 is addressed
      throw buildCalciteContextException(
          "Query bounds with both lower and upper bounds as PRECEDING or FOLLOWING is not supported.",
          windowOrId
      );
    }

    if (plannerContext.queryContext().isWindowingStrictValidation()) {
      if (!targetWindow.isRows() &&
          (!isValidRangeEndpoint(lowerBound) || !isValidRangeEndpoint(upperBound))) {
        // this limitation can be lifted when https://github.com/apache/druid/issues/15767 is addressed
        throw buildCalciteContextException(
            StringUtils.format(
                "The query contains a window frame which may return incorrect results. To disregard this warning, set [%s] to false in the query context.",
                QueryContexts.WINDOWING_STRICT_VALIDATION
            ),
            windowOrId
        );
      }
    }

    super.validateWindow(windowOrId, scope, call);
  }

  @Override
  public void validateInsert(final SqlInsert insert)
  {
    final DruidSqlIngest ingestNode = (DruidSqlIngest) insert;
    if (insert.isUpsert()) {
      throw InvalidSqlInput.exception("UPSERT is not supported.");
    }


    // SQL-style INSERT INTO dst (a, b, c) is not (yet) supported.
    final String operationName = insert.getOperator().getName();
    if (insert.getTargetColumnList() != null) {
      throw InvalidSqlInput.exception(
          "Operation [%s] cannot be run with a target column list, given [%s (%s)]",
          operationName,
          ingestNode.getTargetTable(), ingestNode.getTargetColumnList()
      );
    }

    // The target namespace is both the target table ID and the row type for that table.
    final SqlValidatorNamespace targetNamespace = getNamespace(insert);
    final IdentifierNamespace insertNs = (IdentifierNamespace) targetNamespace;
    // The target is a new or existing datasource.
    final DatasourceTable table = validateInsertTarget(targetNamespace, insertNs, operationName);

    // An existing datasource may have metadata.
    final DatasourceFacade tableMetadata = table == null ? null : table.effectiveMetadata().catalogMetadata();

    // Validate segment granularity, which depends on nothing else.
    if (!(ingestNode.getTargetTable() instanceof ExternalDestinationSqlIdentifier)) {
      Granularity effectiveGranularity = getEffectiveGranularity(operationName, ingestNode, tableMetadata);
      // Note: though this is the validator, we cheat a bit and write the target
      // granularity into the query context. Perhaps this step should be done
      // during conversion, however, we've just worked out the granularity, so we
      // do it here instead.
      try {
        plannerContext.queryContextMap().put(
            DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY,
            plannerContext.getPlannerToolbox().jsonMapper().writeValueAsString(effectiveGranularity)
        );
      }
      catch (JsonProcessingException e) {
        throw InvalidSqlInput.exception(e, "Invalid partition granularity [%s]", effectiveGranularity);
      }
    }

    // The source must be a SELECT
    final SqlNode source = insert.getSource();

    // Validate the source statement.
    // Because of the non-standard Druid semantics, we can't define the target type: we don't know
    // the target columns yet, and we can't infer types when they must come from the SELECT.
    // Normally, the target type is known, and is pushed into the SELECT. In Druid, the SELECT
    // usually defines the target types, unless the catalog says otherwise. Since catalog entries
    // are optional, we don't know the target type until we validate the SELECT. (Also, we won't
    // know names and we match by name.) Thus, we'd have to validate (to know names and types)
    // to get the target types, but we need the target types to validate. Catch-22. So, we punt.
    final SqlValidatorScope scope;
    if (source instanceof SqlSelect) {
      final SqlSelect sqlSelect = (SqlSelect) source;
      validateSelect(sqlSelect, unknownType);
      scope = null;
    } else {
      scope = scopes.get(source);
      validateQuery(source, scope, unknownType);
    }

    final SqlValidatorNamespace sourceNamespace = namespaces.get(source);
    final RelRecordType sourceType = (RelRecordType) sourceNamespace.getRowType();

    // Determine the output (target) schema.
    final RelDataType targetType = validateTargetType(scope, insertNs, insert, sourceType, tableMetadata);

    // Set the type for the INSERT/REPLACE node
    setValidatedNodeType(insert, targetType);

    // Segment size
    if (tableMetadata != null && !plannerContext.queryContextMap().containsKey(CTX_ROWS_PER_SEGMENT)) {
      final Integer targetSegmentRows = tableMetadata.targetSegmentRows();
      if (targetSegmentRows != null) {
        plannerContext.queryContextMap().put(CTX_ROWS_PER_SEGMENT, targetSegmentRows);
      }
    }
  }

  /**
   * Validate the target table. Druid {@code INSERT/REPLACE} can create a new datasource,
   * or insert into an existing one. If the target exists, it must be a datasource. If it
   * does not exist, the target must be in the datasource schema, normally "druid".
   */
  private DatasourceTable validateInsertTarget(
      final SqlValidatorNamespace targetNamespace,
      final IdentifierNamespace insertNs,
      final String operationName
  )
  {
    // Get the target table ID
    final SqlIdentifier destId = insertNs.getId();
    if (destId.names.isEmpty()) {
      // I don't think this can happen, but include a branch for it just in case.
      throw InvalidSqlInput.exception("Operation [%s] requires a target table", operationName);
    }

    // Druid does not support 3+ part names.
    final int n = destId.names.size();
    if (n > 2) {
      throw InvalidSqlInput.exception("Druid does not support 3+ part names: [%s]", destId, operationName);
    }
    String tableName = destId.names.get(n - 1);

    // If this is a 2-part name, the first part must be the datasource schema.
    if (n == 2 && !plannerContext.getPlannerToolbox().druidSchemaName().equals(destId.names.get(0))) {
      throw InvalidSqlInput.exception(
          "Table [%s] does not support operation [%s] because it is not a Druid datasource",
          destId,
          operationName
      );
    }
    try {
      // Try to resolve the table. Will fail if this is an INSERT into a new table.
      validateNamespace(targetNamespace, unknownType);
      SqlValidatorTable target = insertNs.resolve().getTable();
      try {
        return target.unwrap(DatasourceTable.class);
      }
      catch (Exception e) {
        throw InvalidSqlInput.exception(
            "Table [%s] does not support operation [%s] because it is not a Druid datasource",
            destId,
            operationName
        );
      }
    }
    catch (CalciteContextException e) {
      // Something failed. Let's make sure it was the table lookup.
      // The check is kind of a hack, but its the best we can do given that Calcite
      // didn't expect this non-SQL use case.
      if (e.getCause() instanceof SqlValidatorException && e.getMessage()
          .contains(StringUtils.format("Object '%s' not found", tableName))) {
        // The catalog implementation may be "strict": and require that the target
        // table already exists, rather than the default "lenient" mode that can
        // create a new table.
        if (plannerContext.getPlannerToolbox().catalogResolver().ingestRequiresExistingTable()) {
          throw InvalidSqlInput.exception("Cannot %s into [%s] because it does not exist", operationName, destId);
        }
        // New table. Validate the shape of the name.
        IdUtils.validateId("table", tableName);
        return null;
      }
      throw e;
    }
  }

  /**
   * Gets the effective PARTITIONED BY granularity. Resolves the granularity from the granularity specified on the
   * ingest node, and on the table metadata as stored in catalog, if any. Mismatches between the 2 granularities are
   * allowed if both are specified. The granularity specified on the ingest node is taken to be the effective
   * granulartiy if specified. If no granulartiy is specified on either the ingestNode or in the table catalog entry
   * for the table, an error is thrown.
   *
   * @param operationName The operation name
   * @param ingestNode    The ingest node.
   * @param tableMetadata The table metadata as stored in the catalog, if any.
   *
   * @return The effective granularity
   * @throws org.apache.druid.error.DruidException indicating invalud Sql if both the ingest node and table metadata
   * for the respective target table have no PARTITIONED BY granularity defined.
   */
  private Granularity getEffectiveGranularity(
      final String operationName,
      final DruidSqlIngest ingestNode,
      @Nullable final DatasourceFacade tableMetadata
  )
  {
    Granularity effectiveGranularity = null;
    final Granularity ingestionGranularity = ingestNode.getPartitionedBy() != null
        ? ingestNode.getPartitionedBy().getGranularity()
        : null;
    if (ingestionGranularity != null) {
      DruidSqlParserUtils.validateSupportedGranularityForPartitionedBy(ingestNode, ingestionGranularity);
      effectiveGranularity = ingestionGranularity;
    } else {
      final Granularity definedGranularity = tableMetadata == null
          ? null
          : tableMetadata.segmentGranularity();
      if (definedGranularity != null) {
        // Should already have been checked when creating the catalog entry
        DruidSqlParserUtils.validateSupportedGranularityForPartitionedBy(null, definedGranularity);
        effectiveGranularity = definedGranularity;
      }
    }

    if (effectiveGranularity == null) {
      throw InvalidSqlInput.exception(
          "Operation [%s] requires a PARTITIONED BY to be explicitly defined, but none was found.",
          operationName);
    }

    return effectiveGranularity;
  }

  /**
   * Compute and validate the target type. In normal SQL, the engine would insert
   * a project operator after the SELECT before the write to cast columns from the
   * input type to the (compatible) defined output type. Druid doesn't work that way.
   * In MSQ, the output the just is the input type. If the user wants to control the
   * output type, then the user must manually insert any required CAST: Druid is not
   * in the business of changing the type to suit the catalog.
   * <p>
   * As a result, we first propagate column names and types using Druid rules: the
   * output is exactly what SELECT says it is. We then apply restrictions from the
   * catalog. If the table is strict, only column names from the catalog can be
   * used.
   */
  private RelDataType validateTargetType(
      SqlValidatorScope scope,
      final IdentifierNamespace insertNs,
      SqlInsert insert,
      RelRecordType sourceType,
      DatasourceFacade tableMetadata
  )
  {
    final List<RelDataTypeField> sourceFields = sourceType.getFieldList();
    for (final RelDataTypeField sourceField : sourceFields) {
      // Check that there are no unnamed columns in the insert.
      if (UNNAMED_COLUMN_PATTERN.matcher(sourceField.getName()).matches()) {
        throw InvalidSqlInput.exception(
            "Insertion requires columns to be named, but at least one of the columns was unnamed.  This is usually "
            + "the result of applying a function without having an AS clause, please ensure that all function calls"
            + "are named with an AS clause as in \"func(X) as myColumn\"."
        );
      }
    }
    if (tableMetadata == null) {
      return sourceType;
    }
    final boolean isStrict = tableMetadata.isSealed();
    final List<Map.Entry<String, RelDataType>> fields = new ArrayList<>();
    for (RelDataTypeField sourceField : sourceFields) {
      final String colName = sourceField.getName();
      final DatasourceFacade.ColumnFacade definedCol = tableMetadata.column(colName);
      if (definedCol == null) {
        if (isStrict) {
          throw InvalidSqlInput.exception(
              "Column [%s] is not defined in the target table [%s] strict schema",
              colName,
              insert.getTargetTable()
          );
        }

        // Table is not strict: add a new column based on the SELECT column.
        fields.add(Pair.of(colName, sourceField.getType()));
        continue;
      }

      // If the column name is defined, but no type is given then, use the
      // column type from SELECT.
      if (!definedCol.hasType()) {
        fields.add(Pair.of(colName, sourceField.getType()));
        continue;
      }

      // Both the column name and type are provided. Use the name and type
      // from the catalog.
      // Note to future readers: this check is preliminary. It works for the
      // simple column types and has not yet been extended to complex types, aggregates,
      // types defined in extensions, etc. It may be that SQL
      // has types that Druid cannot store. This may crop up with types defined in
      // extensions which are not loaded. Those details are not known at the time
      // of this code so we are not yet in a position to make the right decision.
      // This is a task to be revisited when we have more information.
      final String sqlTypeName = definedCol.sqlStorageType();
      if (sqlTypeName == null) {
        // Don't know the storage type. Just skip this one: Druid types are
        // fluid so let Druid sort out what to store. This is probably not the right
        // answer, but should avoid problems until full type system support is completed.
        fields.add(Pair.of(colName, sourceField.getType()));
        continue;
      }
      RelDataType relType = typeFactory.createSqlType(SqlTypeName.get(sqlTypeName));
      fields.add(Pair.of(
          colName,
          typeFactory.createTypeWithNullability(relType, true)
      ));
    }

    // Perform the SQL-standard check: that the SELECT column can be
    // converted to the target type. This check is retained to mimic SQL
    // behavior, but doesn't do anything because we enforced exact type
    // matches above.
    final RelDataType targetType = typeFactory.createStructType(fields);
    final SqlValidatorTable target = insertNs.resolve().getTable();
    checkTypeAssignment(scope, target, sourceType, targetType, insert);
    return targetType;
  }

  private boolean isPrecedingOrFollowing(@Nullable SqlNode bound)
  {
    if (bound == null) {
      return false;
    }
    SqlKind kind = bound.getKind();
    return kind == SqlKind.PRECEDING || kind == SqlKind.FOLLOWING;
  }

  /**
   * Checks if the given endpoint is acceptable.
   */
  private boolean isValidEndpoint(@Nullable SqlNode bound)
  {
    if (isValidRangeEndpoint(bound)) {
      return true;
    }
    if (bound.getKind() == SqlKind.FOLLOWING || bound.getKind() == SqlKind.PRECEDING) {
      final SqlNode boundVal = ((SqlCall) bound).operand(0);
      if (SqlUtil.isLiteral(boundVal)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks if the given endpoint is valid for a RANGE window frame.
   */
  private boolean isValidRangeEndpoint(@Nullable SqlNode bound)
  {
    return bound == null
        || SqlWindow.isCurrentRow(bound)
        || SqlWindow.isUnboundedFollowing(bound)
        || SqlWindow.isUnboundedPreceding(bound);
  }

  @Override
  public void validateCall(SqlCall call, SqlValidatorScope scope)
  {
    if (call.getKind() == SqlKind.OVER) {
      if (!plannerContext.featureAvailable(EngineFeature.WINDOW_FUNCTIONS)) {
        throw buildCalciteContextException(
            StringUtils.format(
                "The query contains window functions; To run these window functions, specify [%s] in query context.",
                PlannerContext.CTX_ENABLE_WINDOW_FNS),
            call);
      }
    }
    if (call.getKind() == SqlKind.NULLS_FIRST) {
      SqlNode op0 = call.getOperandList().get(0);
      if (op0.getKind() == SqlKind.DESCENDING) {
        throw buildCalciteContextException("DESCENDING ordering with NULLS FIRST is not supported!", call);
      }
    }
    if (call.getKind() == SqlKind.NULLS_LAST) {
      SqlNode op0 = call.getOperandList().get(0);
      if (op0.getKind() != SqlKind.DESCENDING) {
        throw buildCalciteContextException("ASCENDING ordering with NULLS LAST is not supported!", call);
      }
    }
    super.validateCall(call, scope);
  }

  private CalciteContextException buildCalciteContextException(String message, SqlNode call)
  {
    SqlParserPos pos = call.getParserPosition();
    return new CalciteContextException(message,
        new CalciteException(message, null),
        pos.getLineNum(),
        pos.getColumnNum(),
        pos.getEndLineNum(),
        pos.getEndColumnNum());
  }
}
