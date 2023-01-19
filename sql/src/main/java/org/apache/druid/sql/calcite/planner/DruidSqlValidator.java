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
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
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
import org.apache.calcite.util.Pair;
import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.facade.DatasourceFacade;
import org.apache.druid.catalog.model.facade.DatasourceFacade.ColumnFacade;
import org.apache.druid.catalog.model.table.ClusterKeySpec;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.sql.calcite.parser.DruidSqlIngest;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.apache.druid.sql.calcite.parser.DruidSqlParserUtils;
import org.apache.druid.sql.calcite.table.DatasourceTable;
import org.apache.druid.utils.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
  public void validateInsert(SqlInsert insert)
  {
    DruidSqlIngest ingestNode = (DruidSqlIngest) insert;
    if (insert.isUpsert()) {
      throw new IAE("UPSERT is not supported.");
    }

    // SQL-style INSERT INTO dst (a, b, c) is not (yet) supported.
    String operationName = insert.getOperator().getName();
    if (insert.getTargetColumnList() != null) {
      throw new IAE("%s with a target column list is not supported.", operationName);
    }

    // The target namespace is both the target table ID and the row type for that table.
    final SqlValidatorNamespace targetNamespace = getNamespace(insert);
    IdentifierNamespace insertNs = (IdentifierNamespace) targetNamespace;

    // The target is a new or existing datasource.
    DatasourceTable table = validateInsertTarget(targetNamespace, insertNs, operationName);

    // An existing datasource may have metadata.
    DatasourceFacade tableMetadata = table == null ? null : table.effectiveMetadata().catalogMetadata();

    // Validate segment granularity, which depends on nothing else.
    validateSegmentGranularity(operationName, ingestNode, tableMetadata);

    // The source must be a SELECT
    SqlSelect select = validateInsertSelect(insert, operationName);

    // Convert CLUSTERED BY, or the catalog equivalent, to an ORDER BY clause
    SqlNodeList catalogClustering = convertCatalogClustering(tableMetadata);
    rewriteClusteringToOrderBy(select, ingestNode, catalogClustering);

    // Validate the source statement. Validates the ORDER BY pushed down in the above step.
    validateSelect(select, unknownType);
    SqlValidatorScope selectScope = getSelectScope(select);

    SqlValidatorNamespace sourceNamespace = namespaces.get(select);
    RelRecordType sourceType = (RelRecordType) sourceNamespace.getRowType();

    // Validate the __time column
    int timeColumnIndex = sourceType.getFieldNames().indexOf(Columns.TIME_COLUMN);
    if (timeColumnIndex != -1) {
      validateTimeColumn(sourceType, timeColumnIndex);
    }

    // Validate clustering against the SELECT row type. Clustering has additional
    // constraints beyond what was validated for the pushed-down ORDER BY.
    // Though we pushed down clustering above, only now can we validate it after
    // we've determined the SELECT row type.
    validateClustering(sourceType, timeColumnIndex, ingestNode, catalogClustering);

    // Determine the output (target) schema.
    RelDataType targetType = validateTargetType(insert, sourceType, tableMetadata);

    // Set the type for the INSERT/REPLACE node
    setValidatedNodeType(insert, targetType);

    // Segment size
    if (tableMetadata != null) {
      Integer targetSegmentRows = tableMetadata.targetSegmentRows();
      if (targetSegmentRows != null) {
        validatorContext.queryContextMap().put(CTX_ROWS_PER_SEGMENT, targetSegmentRows);
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
    SqlIdentifier destId = insertNs.getId();
    if (destId.names.isEmpty()) {
      // I don't think this can happen, but include a branch for it just in case.
      throw new IAE("%s requires a target table.", operationName);
    }

    // Druid does not support 3+ part names.
    int n = destId.names.size();
    if (n > 2) {
      throw new IAE("Table name is undefined: %s", destId.toString());
    }
    String tableName = destId.names.get(n - 1);

    // If this is a 2-part name, the first part must be the datasource schema.
    if (n == 2 && !validatorContext.druidSchemaName().equals(destId.names.get(0))) {
      throw new IAE("Cannot %s into %s because it is not a Druid datasource.",
          operationName,
          destId
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
        throw new IAE("Cannot %s into %s: it is not a datasource", operationName, destId);
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
          throw new IAE("Cannot %s into %s because it does not exist", operationName, destId);
        }
        // New table. Validate the shape of the name.
        IdUtils.validateId(operationName + " dataSource", tableName);
        return null;
      }
      throw e;
    }
  }

  private void validateSegmentGranularity(
      final String operationName,
      final DruidSqlIngest ingestNode,
      final DatasourceFacade tableMetadata
  )
  {
    final Granularity definedGranularity = tableMetadata == null ? null : tableMetadata.segmentGranularity();
    if (definedGranularity != null) {
      // Should already have been checked when creating the catalog entry
      DruidSqlParserUtils.throwIfUnsupportedGranularityInPartitionedBy(definedGranularity);
    }
    final Granularity ingestionGranularity = DruidSqlParserUtils.convertSqlNodeToGranularity(ingestNode.getPartitionedBy());
    if (ingestionGranularity != null) {
      DruidSqlParserUtils.throwIfUnsupportedGranularityInPartitionedBy(ingestionGranularity);
    }
    final Granularity finalGranularity;
    if (definedGranularity == null && ingestionGranularity == null) {
      // Neither have a value: error
      throw new IAE(
          "%s statements must specify a PARTITIONED BY clause explicitly",
          operationName
      );
    } else if (ingestionGranularity == null) {
      // The query has no granularity: just apply the catalog granularity.
      finalGranularity = definedGranularity;
    } else if (definedGranularity == null) {
      // The catalog has no granularity: apply the query value
      finalGranularity = ingestionGranularity;
    } else if (definedGranularity.equals(ingestionGranularity)) {
      // Both have a setting. They have to be the same.
      finalGranularity = definedGranularity;
    } else {
      throw new IAE(
          "PARTITIONED BY mismatch. Catalog: [%s], query: [%s]",
          granularityToSqlString(definedGranularity),
          granularityToSqlString(ingestionGranularity)
      );
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

  private String granularityToSqlString(Granularity gran)
  {
    if (gran == null) {
      return "NULL";
    }
    if (Granularities.ALL == gran) {
      return "ALL TIME";
    }
    if (!(gran instanceof PeriodGranularity)) {
      return gran.toString();
    }
    return ((PeriodGranularity) gran).getPeriod().toString();
  }

  private SqlSelect validateInsertSelect(
      final SqlInsert insert,
      final String operationName
  )
  {
    SqlNode source = insert.getSource();

    // The source SELECT cannot include an ORDER BY clause. Ordering is given
    // by the CLUSTERED BY clause, if any.
    // Check that an ORDER BY clause is not provided by the underlying query
    SqlNodeList orderByList;
    if (source instanceof SqlOrderBy) {
      throw new IAE(
          "Cannot have ORDER BY on %s %s statement, use CLUSTERED BY instead.",
          "INSERT".equals(operationName) ? "an" : "a",
          operationName
      );
    }

    // Validate the query: but we only know how to do SELECT.
    // As we do, get the scope for the query.
    if (!source.isA(SqlKind.QUERY)) {
      throw new IAE("Cannot execute %s.", source.getKind());
    }
    if (!(source instanceof SqlSelect)) {
      throw new IAE(
          "%s %s must include a SELECT statement",
          "INSERT".equals(operationName) ? "An" : "A",
          operationName
      );
    }
    SqlSelect select = (SqlSelect) source;
    orderByList = select.getOrderList();
    if (orderByList != null && orderByList.size() != 0) {
      throw new IAE(
          "Cannot have ORDER BY on %s %s statement, use CLUSTERED BY instead.",
          "INSERT".equals(operationName) ? "an" : "a",
          operationName
      );
    }
    return select;
  }

  private SqlNodeList convertCatalogClustering(final DatasourceFacade tableMetadata)
  {
    if (tableMetadata == null) {
      return null;
    }
    List<ClusterKeySpec> keyCols = tableMetadata.clusterKeys();
    if (CollectionUtils.isNullOrEmpty(keyCols)) {
      return null;
    }
    SqlNodeList keyNodes = new SqlNodeList(SqlParserPos.ZERO);
    for (ClusterKeySpec keyCol : keyCols) {
      SqlIdentifier colIdent = new SqlIdentifier(
          Collections.singletonList(keyCol.expr()),
          null, SqlParserPos.ZERO,
          Collections.singletonList(SqlParserPos.ZERO)
          );
      SqlNode keyNode;
      if (keyCol.desc()) {
        keyNode = SqlStdOperatorTable.DESC.createCall(SqlParserPos.ZERO, colIdent);
      } else {
        keyNode = colIdent;
      }
      keyNodes.add(keyNode);
    }
    return keyNodes;
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
  private void rewriteClusteringToOrderBy(SqlSelect select, DruidSqlIngest ingestNode, SqlNodeList catalogClustering)
  {
    SqlNodeList clusteredBy = ingestNode.getClusteredBy();
    if (clusteredBy == null || clusteredBy.getList().isEmpty()) {
      if (catalogClustering == null || catalogClustering.getList().isEmpty()) {
        return;
      }
      clusteredBy = catalogClustering;
    }

    // This part is a bit sad. By the time we get here, the validator will have created
    // the ORDER BY namespace if we had a real ORDER BY. We have to "catch up" and do the
    // work that registerQuery() should have done. That's kind of OK. But, the orderScopes
    // variable is private, so we have to play dirty tricks to get at it.
    select.setOrderBy(clusteredBy);
    OrderByScope orderScope = ValidatorShim.newOrderByScope(scopes.get(select), clusteredBy, select);
    try {
      @SuppressWarnings("unchecked")
      Map<SqlSelect, SqlValidatorScope> orderScopes = (Map<SqlSelect, SqlValidatorScope>) FieldUtils.getDeclaredField(SqlValidatorImpl.class, "orderScopes", true).get(this);
      orderScopes.put(select, orderScope);
    }
    catch (Exception e) {
      throw new ISE(e, "orderScopes is not accessible");
    }
  }

  private void validateTimeColumn(RelRecordType sourceType, int timeColumnIndex)
  {
    RelDataTypeField timeCol = sourceType.getFieldList().get(timeColumnIndex);
    RelDataType timeColType = timeCol.getType();
    if (timeColType instanceof BasicSqlType) {
      BasicSqlType timeColSqlType = (BasicSqlType) timeColType;
      SqlTypeName timeColSqlTypeName = timeColSqlType.getSqlTypeName();
      if (timeColSqlTypeName == SqlTypeName.BIGINT || timeColSqlTypeName == SqlTypeName.TIMESTAMP) {
        return;
      }
    }
    throw new IAE(
        "Invalid %s column type %s: must be BIGINT or TIMESTAMP",
        timeCol.getName(),
        timeColType.toString()
    );
  }

  /**
   * Verify clustering which can come from the query, the catalog or both. If both,
   * the two must match. In either case, the cluster keys must be present in the SELECT
   * clause. The {@code __time} column cannot be included.
   * @param source
   */
  private void validateClustering(
      final RelRecordType sourceType,
      final int timeColumnIndex,
      final DruidSqlIngest ingestNode,
      final SqlNodeList catalogClustering
  )
  {
    final SqlNodeList clusteredBy = ingestNode.getClusteredBy();

    // Validate both the catalog and query definitions if present. This ensures
    // that things are sane if we later check that the two are identical.
    if (clusteredBy != null) {
      validateClusteredBy(sourceType, timeColumnIndex, clusteredBy);
    }
    if (catalogClustering != null) {
      // Catalog defines the key columns. Verify that they are present in the query.
      validateClusteredBy(sourceType, timeColumnIndex, catalogClustering);
    }
    if (clusteredBy != null && catalogClustering != null) {
      // Both the query and catalog have keys.
      verifyQueryClusterByMatchesCatalog(sourceType, catalogClustering, clusteredBy);
    }
  }

  /**
   * Validate the CLUSTERED BY list. Members can be any of the following:
   * <p>
   * {@code CLUSTERED BY [<ordinal> | <id> | <expr>] DESC?}
   * <p>
   * Ensure that each id exists. Ensure each column is included only once.
   * For an expression, just ensure it is valid; we don't check for duplicates.
   * <p>
   * Once the keys are validated, update the underlying {@code SELECT} statement
   * to include the {@code CLUSTERED BY} as the {@code ORDER BY} clause.
   */
  private void validateClusteredBy(
      final RelRecordType sourceType,
      final int timeColumnIndex,
      final SqlNodeList clusteredBy
  )
  {
    // Keep track of fields which have been referenced.
    final List<String> fieldNames = sourceType.getFieldNames();
    final int fieldCount = fieldNames.size();
    final boolean[] refs = new boolean[fieldCount];

    // Process cluster keys
    for (SqlNode clusterKey : clusteredBy) {
      Pair<Integer, Boolean> key = resolveClusterKey(clusterKey, fieldNames);
      // If an expression, index is null. Validation was done in the ORDER BY check.
      // Else, do additional MSQ-specific checks.
      if (key != null) {
        int index = key.left;
        // Can't cluster by __time
        if (index == timeColumnIndex) {
          throw new IAE("Do not include %s in the CLUSTERED BY clause: it is managed by PARTITIONED BY", Columns.TIME_COLUMN);
        }
        // No duplicate references
        if (refs[index]) {
          throw new IAE("Duplicate CLUSTERED BY key: %s", clusterKey);
        }
        refs[index] = true;
      }
    }
  }

  private Pair<Integer, Boolean> resolveClusterKey(SqlNode clusterKey, final List<String> fieldNames)
  {
    boolean desc = false;

    // Check if the key is compound: only occurs for DESC. The ASC
    // case is abstracted away by the parser.
    if (clusterKey instanceof SqlBasicCall) {
      SqlBasicCall basicCall = (SqlBasicCall) clusterKey;
      if (basicCall.getOperator() == SqlStdOperatorTable.DESC) {
        // Cluster key is compound: CLUSTERED BY foo DESC
        // We check only the first element
        clusterKey = ((SqlBasicCall) clusterKey).getOperandList().get(0);
        desc = true;
      }
    }

    // We now have the actual key. Handle the three cases.
    if (clusterKey instanceof SqlNumericLiteral) {
      // Key is an ordinal: CLUSTERED BY 2
      // Ordinals are 1-based.
      int ord = ((SqlNumericLiteral) clusterKey).intValue(true);
      int index = ord - 1;

      // The ordinal has to be in range.
      if (index < 0 || fieldNames.size() <= index) {
        throw new IAE("CLUSTERED BY ordinal %d is not valid", ord);
      }
      return new Pair<>(index, desc);
    } else if (clusterKey instanceof SqlIdentifier) {
      // Key is an identifier: CLUSTERED BY foo
      SqlIdentifier key = (SqlIdentifier) clusterKey;

      // Only key of the form foo are allowed, not foo.bar
      if (!key.isSimple()) {
        throw new IAE("CLUSTERED BY keys must be a simple name: '%s'", key.toString());
      }

      // The name must match an item in the select list
      String keyName = key.names.get(0);
      // Slow linear search. We assume that there are not many cluster keys.
      int index = fieldNames.indexOf(keyName);
      if (index == -1) {
        throw new IAE("CLUSTERED BY key column '%s' is not valid", keyName);
      }
      return new Pair<>(index, desc);
    } else {
      // Key is an expression: CLUSTERED BY CEIL(m2)
      return null;
    }
  }

  /**
   * Both the catalog and query define clustering. This is allowed as long as they
   * are identical.
   */
  private void verifyQueryClusterByMatchesCatalog(
      final RelRecordType sourceType,
      final SqlNodeList catalogClustering,
      final SqlNodeList clusteredBy
  )
  {
    if (clusteredBy.size() != catalogClustering.size()) {
      throw clusterKeyMismatchException(catalogClustering, clusteredBy);
    }
    List<String> fieldNames = sourceType.getFieldNames();
    for (int i = 0; i < clusteredBy.size(); i++) {
      SqlNode catalogKey = catalogClustering.get(i);
      SqlNode clusterKey = clusteredBy.get(i);
      Pair<Integer, Boolean> catalogPair = resolveClusterKey(catalogKey, fieldNames);
      Pair<Integer, Boolean> queryPair = resolveClusterKey(clusterKey, fieldNames);

      // Cluster keys in the catalog must be field references. If unresolved,
      // we would have gotten an error above. Here we make sure that both
      // indexes are the same. Since the catalog index can't be null, we're
      // essentially checking that the indexes are the same: they name the same
      // column.
      if (!Objects.equals(catalogPair, queryPair)) {
        throw clusterKeyMismatchException(catalogClustering, clusteredBy);
      }
    }
  }

  private RuntimeException clusterKeyMismatchException(SqlNodeList catalogClustering, SqlNodeList clusterKeys)
  {
    throw new IAE(
        "CLUSTER BY mismatch. Catalog: [%s], query: [%s]",
        catalogClustering,
        clusterKeys
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
   * used. If a type is provided, then the <i>user</i> (not the planner) is responsible
   * for adding a CAST as needed so that the SELECT column type exactly matches the
   * catalog column type, if given.
   */
  private RelDataType validateTargetType(SqlInsert insert, RelRecordType sourceType, DatasourceFacade tableMetadata)
  {
    final List<RelDataTypeField> sourceFields = sourceType.getFieldList();
    for (RelDataTypeField sourceField : sourceFields) {
      String colName = sourceField.getName();
      // Check that there are no unnamed columns in the insert.
      if (UNNAMED_COLUMN_PATTERN.matcher(colName).matches()) {
        throw new IAE("Expressions must provide an alias to specify the target column: func(X) AS myColumn");
      }
    }
    if (tableMetadata == null) {
      return sourceType;
    }
    final boolean isStrict = tableMetadata.isSealed();
    final List<Map.Entry<String, RelDataType>> fields = new ArrayList<>();
    for (RelDataTypeField sourceField : sourceFields) {
      String colName = sourceField.getName();
      ColumnFacade definedCol = tableMetadata.column(colName);
      if (definedCol == null) {
        // No catalog definition for this column.
        if (isStrict) {
          // Table is strict: cannot add new columns at ingest time.
          throw new IAE(
              "Target column \"%s\".\"%s\" is not defined",
              insert.getTargetTable(),
              colName
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
      // TODO: Handle error if type not found
      String sqlTypeName = definedCol.sqlStorageType();
      if (sqlTypeName == null) {
        // Don't know the storage type. Just skip this one: Druid types are
        // fluid so let Druid sort out what to store.
        fields.add(Pair.of(colName, sourceField.getType()));
        continue;
      }
      SqlTypeName sqlType = SqlTypeName.get(sqlTypeName);
      RelDataType targetType = typeFactory.createSqlType(sqlType);

      // Druid-specific check: the user, not Druid, is responsible for casting
      // the column to the correct type. Druid ignores nullability and precision.
      // Focus only on type type name. This also handle the fact that strings and
      // string arrays are the same, and the complex types are all treated the same
      // independent of underlying type.
      if (!sourceField.getType().getSqlTypeName().equals(targetType.getSqlTypeName())) {
        throw new IAE("Type %s of column %s does not match the defined type of %s: add a CAST",
            sourceField.getType().getSqlTypeName(),
            colName,
            targetType.getSqlTypeName()
        );
      }
      fields.add(Pair.of(colName, targetType));
    }

    // Perform the SQL-standard check: that the SELECT column can be
    // converted to the target type. This check is retained to mimic SQL
    // behavior, but doesn't do anything because we enforced exact type
    // matches above.
    RelDataType targetType = typeFactory.createStructType(fields);
    checkTypeAssignment(sourceType, targetType, insert);
    return targetType;
  }
}
