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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.IdentifierNamespace;
import org.apache.calcite.sql.validate.OrderByScope;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.apache.calcite.sql.validate.ValidatorShim;
import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.sql.calcite.parser.DruidSqlIngest;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.apache.druid.sql.calcite.parser.DruidSqlParserUtils;
import org.apache.druid.sql.calcite.table.DatasourceTable;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Druid extended SQL validator.
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

  private final ValidatorContext validatorContext;

  protected DruidSqlValidator(
      final SqlOperatorTable opTab,
      final CalciteCatalogReader catalogReader,
      final JavaTypeFactory typeFactory,
      final SqlConformance conformance,
      final ValidatorContext validatorContext
  )
  {
    super(opTab, catalogReader, typeFactory, conformance);
    this.validatorContext = validatorContext;
  }

  /**
   * Druid-specific validation for an INSERT statement. In Druid, the columns are
   * matched by name. A datasource, by default, allows the insertion of arbitrary columns,
   * but the catalog may enforce a strict schema (all columns must exist). Destination
   * types are set by the catalog, where available, else by the query.
   * <p>
   * The Druid {@code INSERT} statement is non-standard in a variety of ways:
   * <ul>
   * <li>Allows the target table to not yet exist. Instead, {@code INSERT}
   * creates it.</li>
   * <li>Does not allow specifying the list of columns:
   * {@code INSERT INTO dst (a, b, c) ...}</li>
   * <li>When given without target columns (the only form allowed), columns are
   * not matched by schema position as in standard SQL, but rather by name.</li>
   * <li>There is no requirement that the target columns already exist. In fact,
   * even if the target column exists, any existing type is ignored if not specified
   * in the catalog.</li>
   * <li>The source can only be a {@code SELECT} statement, not {@code VALUES}.</li>
   * <li>Types thus propagate upwards from the {@code SELECT} to the the target
   * table. Standard SQL says that types propagate down from the target table to the
   * source.</li>
   * <li>The __time column is special in multiple ways.</li>
   * <li>Includes the {@code CLUSTERED BY} and {@code PARTITIONED BY} clauses.</li>
   * </ul>
   * The result is that the validation for the Druid {@code INSERT} is wildly customized
   * relative to standard SQL.
   */
  // TODO: Ensure the source and target are not the same
  @Override
  public void validateInsert(final SqlInsert insert)
  {
    final DruidSqlIngest ingestNode = (DruidSqlIngest) insert;
    if (insert.isUpsert()) {
      throw new IAE("UPSERT is not supported.");
    }

    // SQL-style INSERT INTO dst (a, b, c) is not (yet) supported.
    final String operationName = insert.getOperator().getName();
    if (insert.getTargetColumnList() != null) {
      throw new IAE("%s with a target column list is not supported.", operationName);
    }

    // The target namespace is both the target table ID and the row type for that table.
    final SqlValidatorNamespace targetNamespace = getNamespace(insert);
    final IdentifierNamespace insertNs = (IdentifierNamespace) targetNamespace;

    // The target is a new or existing datasource.
    validateInsertTarget(targetNamespace, insertNs, operationName);

    // Validate segment granularity, which depends on nothing else.
    validateSegmentGranularity(operationName, ingestNode);

    // The source must be a SELECT
    final SqlNode source = insert.getSource();
    ensureNoOrderBy(source, operationName);

    // Convert CLUSTERED BY to an ORDER BY clause
    // Note that by so doing, we allow CLUSTERED BY to reference input columns,
    // including those not in the output. Doing so causes issues with compaction
    // (Compaction can't reference input columns that no longer exist), but early
    // versions of MSQ supported this feature and we cannot remove it.
    rewriteClusteringToOrderBy(source, ingestNode);

    // Validate the source statement. Validates the ORDER BY pushed down in the above step.
    // Because of the non-standard Druid semantics, we can't define the target type: we don't know
    // the target columns yet, and we can't infer types when they must come from the SELECT.
    // Normally, the target type is known, and is pushed into the SELECT. In Druid, the SELECT
    // usually defines the target types, unless the catalog says otherwise. Since catalog entries
    // are optional, we don't know the target type until we validate the SELECT. (Also, we won't
    // know names and we match by name.) Thus, we'd have to validate (to know names and types)
    // to get the target types, but we need the target types to validate. Catch-22. So, we punt.
    if (source instanceof SqlSelect) {
      final SqlSelect sqlSelect = (SqlSelect) source;
      validateSelect(sqlSelect, unknownType);
    } else {
      final SqlValidatorScope scope = scopes.get(source);
      validateQuery(source, scope, unknownType);
    }

    final SqlValidatorNamespace sourceNamespace = namespaces.get(source);
    final RelRecordType sourceType = (RelRecordType) sourceNamespace.getRowType();

    // Validate the __time column
    int timeColumnIndex = sourceType.getFieldNames().indexOf(Columns.TIME_COLUMN);
    if (timeColumnIndex != -1) {
      validateTimeColumn(sourceType, timeColumnIndex);
    }

    // Determine the output (target) schema.
    final RelDataType targetType = validateTargetType(sourceType);

    // Set the type for the INSERT/REPLACE node
    setValidatedNodeType(insert, targetType);
  }

  /**
   * Validate the target table. Druid {@code INSERT/REPLACE} can create a new datasource,
   * or insert into an existing one. If the target exists, it must be a datasource. If it
   * does not exist, the target must be in the datasource schema, normally "druid".
   */
  private void validateInsertTarget(
      final SqlValidatorNamespace targetNamespace,
      final IdentifierNamespace insertNs,
      final String operationName
  )
  {
    // Get the target table ID
    final SqlIdentifier destId = insertNs.getId();
    if (destId.names.isEmpty()) {
      // I don't think this can happen, but include a branch for it just in case.
      throw new IAE("%s requires a target table.", operationName);
    }

    // Druid does not support 3+ part names.
    final int n = destId.names.size();
    if (n > 2) {
      throw new IAE("Druid does not support 3+ part names: [%s]", destId);
    }
    String tableName = destId.names.get(n - 1);

    // If this is a 2-part name, the first part must be the datasource schema.
    if (n == 2 && !validatorContext.druidSchemaName().equals(destId.names.get(0))) {
      throw new IAE("Cannot %s into [%s] because the table is not in the 'druid' schema",
          operationName,
          destId
      );
    }
    try {
      // Try to resolve the table. Will fail if this is an INSERT into a new table.
      validateNamespace(targetNamespace, unknownType);
      SqlValidatorTable target = insertNs.resolve().getTable();
      try {
        target.unwrap(DatasourceTable.class);
        return;
      }
      catch (Exception e) {
        throw new IAE("Cannot %s into [%s] because it is not a datasource", operationName, destId);
      }
    }
    catch (CalciteContextException e) {
      // Something failed. Let's make sure it was the table lookup.
      // The check is kind of a hack, but its the best we can do given that Calcite
      // didn't expect this non-SQL use case.
      if (e.getCause() instanceof SqlValidatorException && e.getMessage().contains(StringUtils.format("Object '%s' not found", tableName))) {
        // The catalog implementation may be "strict": and require that the target
        // table already exists, rather than the default "lenient" mode that can
        // create a new table.
        if (validatorContext.catalog().ingestRequiresExistingTable()) {
          throw new IAE("Cannot %s into [%s] because it does not exist", operationName, destId);
        }
        // New table. Validate the shape of the name.
        IdUtils.validateId(operationName + " dataSource", tableName);
        return;
      }
      throw e;
    }
  }

  private void validateSegmentGranularity(
      final String operationName,
      final DruidSqlIngest ingestNode
  )
  {
    final Granularity ingestionGranularity = DruidSqlParserUtils.convertSqlNodeToGranularity(ingestNode.getPartitionedBy());
    if (ingestionGranularity != null) {
      DruidSqlParserUtils.throwIfUnsupportedGranularityInPartitionedBy(ingestionGranularity);
    }
    final Granularity finalGranularity;
    // The catalog has no granularity: apply the query value
    if (ingestionGranularity == null) {
      // Neither have a value: error
      throw new IAE(
          "%s statements must specify a PARTITIONED BY clause explicitly",
          operationName
      );
    } else {
      finalGranularity = ingestionGranularity;
    }

    // Note: though this is the validator, we cheat a bit and write the target
    // granularity into the query context. Perhaps this step should be done
    // during conversion, however, we've just worked out the granularity, so we
    // do it here instead.
    try {
      validatorContext.queryContextMap().put(
          DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY,
          validatorContext.jsonMapper().writeValueAsString(finalGranularity)
      );
    }
    catch (JsonProcessingException e) {
      throw new IAE("Invalid PARTITIONED BY granularity");
    }
  }

  private void ensureNoOrderBy(
      SqlNode source,
      final String operationName
  )
  {
    // The source SELECT cannot include an ORDER BY clause. Ordering is given
    // by the CLUSTERED BY clause, if any.
    // Check that an ORDER BY clause is not provided by the underlying query
    SqlNodeList orderByList;
    if (source instanceof SqlOrderBy) {
      throw new IAE(
          "Cannot have ORDER BY on %s %s statement, use CLUSTERED BY instead.",
          statementArticle(operationName),
          operationName
      );
    }

    // Pull the SELECT statement out of the WITH clause
    if (source instanceof SqlWith) {
      source = ((SqlWith) source).getOperandList().get(1);
    }
    // If the child of INSERT or WITH is not SELECT, then the statement is not valid.
    if (!(source instanceof SqlSelect)) {
      throw new IAE(
          "%s is not supported within %s %s statement.",
          source.getKind(),
          statementArticle(operationName),
          operationName
      );
    }

    // Verify that the SELECT has no ORDER BY clause
    SqlSelect select = (SqlSelect) source;
    orderByList = select.getOrderList();
    if (orderByList != null && orderByList.size() != 0) {
      throw new IAE(
          "ORDER BY is not supported within %s %s statement, use CLUSTERED BY instead.",
          statementArticle(operationName),
          operationName
      );
    }
  }

  private String statementArticle(final String operationName)
  {
    return "INSERT".equals(operationName) ? "an" : "a";
  }

  // This part is a bit sad. By the time we get here, the validator will have created
  // the ORDER BY namespace if we had a real ORDER BY. We have to "catch up" and do the
  // work that registerQuery() should have done. That's kind of OK. But, the orderScopes
  // variable is private, so we have to play dirty tricks to get at it.
  //
  // Warning: this may no longer work if Java forbids access to private fields in a
  // future release.
  private static final Field ORDER_SCOPES_FIELD;

  static {
    try {
      ORDER_SCOPES_FIELD = FieldUtils.getDeclaredField(
              SqlValidatorImpl.class,
              "orderScopes",
              true
          );
    }
    catch (RuntimeException e) {
      throw new ISE(e, "SqlValidatorImpl.orderScopes is not accessible");
    }
  }

  /**
   * Clustering is a kind of ordering. We push the CLUSTERED BY clause into the source query as
   * an ORDER BY clause. In an ideal world, clustering would be outside of SELECT: it is an operation
   * applied after the data is selected. For now, we push it into the SELECT, then MSQ pulls it back
   * out. This is unfortunate as errors will be attributed to ORDER BY, which the user did not
   * actually specify (it is an error to do so.) However, with the current hybrid structure, it is
   * not possible to add the ORDER by later: doing so requires access to the order by namespace
   * which is not visible to subclasses.
   */
  private void rewriteClusteringToOrderBy(SqlNode source, DruidSqlIngest ingestNode)
  {
    SqlNodeList clusteredBy = ingestNode.getClusteredBy();
    while (source instanceof SqlWith) {
      source = ((SqlWith) source).getOperandList().get(1);
    }
    final SqlSelect select = (SqlSelect) source;

    select.setOrderBy(clusteredBy);
    final OrderByScope orderScope = ValidatorShim.newOrderByScope(scopes.get(select), clusteredBy, select);
    try {
      @SuppressWarnings("unchecked")
      final Map<SqlSelect, SqlValidatorScope> orderScopes =
          (Map<SqlSelect, SqlValidatorScope>) ORDER_SCOPES_FIELD.get(this);
      orderScopes.put(select, orderScope);
    }
    catch (Exception e) {
      throw new ISE(e, "orderScopes is not accessible");
    }
  }

  private void validateTimeColumn(
      final RelRecordType sourceType,
      final int timeColumnIndex
  )
  {
    final RelDataTypeField timeCol = sourceType.getFieldList().get(timeColumnIndex);
    final RelDataType timeColType = timeCol.getType();
    if (timeColType instanceof BasicSqlType) {
      final BasicSqlType timeColSqlType = (BasicSqlType) timeColType;
      final SqlTypeName timeColSqlTypeName = timeColSqlType.getSqlTypeName();
      if (timeColSqlTypeName == SqlTypeName.BIGINT || timeColSqlTypeName == SqlTypeName.TIMESTAMP) {
        return;
      }
    }
    throw new IAE(
        "Column [%s] is being used as the time column. It must be of type BIGINT or TIMESTAMP, got [%s]",
        timeCol.getName(),
        timeColType
    );
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
  private RelDataType validateTargetType(RelRecordType sourceType)
  {
    final List<RelDataTypeField> sourceFields = sourceType.getFieldList();
    for (int i = 0; i < sourceFields.size(); i++) {
      final RelDataTypeField sourceField = sourceFields.get(i);
      // Check that there are no unnamed columns in the insert.
      if (UNNAMED_COLUMN_PATTERN.matcher(sourceField.getName()).matches()) {
        throw new IAE(
            "Expression [%d] must provide an alias to specify the target column: func(X) AS myColumn",
            i + 1
        );
      }
    }
    return sourceType;
  }
}
