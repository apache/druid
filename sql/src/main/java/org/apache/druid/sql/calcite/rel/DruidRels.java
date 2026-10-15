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

package org.apache.druid.sql.calcite.rel;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.druid.error.DruidException;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.ProjectDataSource;
import org.apache.druid.query.RestrictedDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.querygen.SourceDescProducer.SourceDesc;
import org.apache.druid.sql.calcite.table.DruidTable;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class DruidRels
{
  /**
   * Returns the DataSource involved in a leaf query of class {@link DruidQueryRel}.
   */
  public static Optional<DruidTable> druidTableIfLeafRel(final DruidRel<?> druidRel)
  {
    if (druidRel instanceof DruidQueryRel) {
      return Optional.of(((DruidQueryRel) druidRel).getDruidTable());
    } else {
      return Optional.empty();
    }
  }

  /**
   * Check if a druidRel is a simple table scan, or a projection that merely remaps columns without transforming them.
   * Like {@link #isScanOrProject} but more restrictive: only remappings are allowed.
   *
   * @param druidRel         the rel to check
   * @param canBeJoinOrUnion consider a {@link DruidJoinQueryRel} or {@link DruidUnionDataSourceRel} as possible
   *                         scans-and-mappings too.
   */
  public static boolean isScanOrMapping(final DruidRel<?> druidRel, final boolean canBeJoinOrUnion)
  {
    if (isScanOrProject(druidRel, canBeJoinOrUnion)) {
      // Like isScanOrProject, but don't allow transforming projections.
      final PartialDruidQuery partialQuery = druidRel.getPartialDruidQuery();
      return partialQuery.getSelectProject() == null || partialQuery.getSelectProject().isMapping();
    } else {
      return false;
    }
  }

  /**
   * Check if a druidRel is a simple table scan or a scan + projection.
   *
   * @param druidRel         the rel to check
   * @param canBeJoinOrUnion consider a {@link DruidJoinQueryRel} or {@link DruidUnionDataSourceRel} as possible
   *                         scans-and-mappings too.
   */
  public static boolean isScanOrProject(final DruidRel<?> druidRel, final boolean canBeJoinOrUnion)
  {
    if (druidRel instanceof DruidQueryRel || (canBeJoinOrUnion && (druidRel instanceof DruidJoinQueryRel || druidRel instanceof DruidCorrelateUnnestRel
                                                                   || druidRel instanceof DruidUnionDataSourceRel))) {
      final PartialDruidQuery partialQuery = druidRel.getPartialDruidQuery();
      final PartialDruidQuery.Stage stage = partialQuery.stage();
      return (stage == PartialDruidQuery.Stage.SCAN || stage == PartialDruidQuery.Stage.SELECT_PROJECT)
             && partialQuery.getWhereFilter() == null;
    } else {
      return false;
    }
  }

  /**
   * Check if a druidRel is a leaf table scan that does nothing beyond filtering and projecting.
   *
   * @param druidRel the rel to check
   */
  public static boolean isScanOrFilterOrProject(final DruidRel<?> druidRel)
  {
    if (!(druidRel instanceof DruidQueryRel)) {
      return false;
    }

    final PartialDruidQuery.Stage stage = druidRel.getPartialDruidQuery().stage();

    return stage == PartialDruidQuery.Stage.SCAN
           || stage == PartialDruidQuery.Stage.WHERE_FILTER
           || stage == PartialDruidQuery.Stage.SELECT_PROJECT;
  }

  /**
   * Returns a {@link UnionDataSource} arm for a query over a table, with the query's filter and projection moved into
   * the datasource, or null if the query does not read a table.
   *
   * @param query         the arm's query, which must only scan, filter, and project
   * @param baseSignature signature of the table the query reads
   */
  @Nullable
  public static SourceDesc makeUnionArm(final DruidQuery query, final RowSignature baseSignature)
  {
    // Only the filter and projection carry over into the arm, so anything else the query does would be lost.
    if (!(query.getQuery() instanceof ScanQuery scanQuery)
        || scanQuery.isLimited()
        || scanQuery.getScanRowsOffset() > 0
        || !scanQuery.getOrderBys().isEmpty()) {
      throw DruidException.defensive(
          "Union arm must only scan, filter, and project, but got query[%s]",
          query.getQuery()
      );
    }

    if (!isTable(query.getDataSource())) {
      return null;
    }

    // Policies should have been applied before calling this method. Verify defensively.
    verifyHasPoliciesApplied(query);

    // The arm's filter and projection go into its datasource, under the arm's own column names.
    return new SourceDesc(
        DruidQuery.makeFilteredProjectedDataSource(
            query.getDataSource(),
            baseSignature,
            query.getFilter(),
            query.getScanVirtualColumns()
        ),
        query.getOutputRowSignature()
    );
  }

  /**
   * Checks if the provided query has had policies applied, and throws a defensive exception if not.
   */
  private static void verifyHasPoliciesApplied(final DruidQuery druidQuery)
  {
    final PlannerContext plannerContext = druidQuery.getPlannerContext();
    final DataSource dataSourceWithPolicies = druidQuery.getDataSource().withPolicies(
        plannerContext.getAuthorizationResult().getPolicyMap(),
        plannerContext.getPlannerToolbox().getPolicyEnforcer()
    );

    if (!dataSourceWithPolicies.equals(druidQuery.getDataSource())) {
      throw DruidException.defensive(
          "Policies must be applied before calling this method; got[%s] but expected[%s]",
          druidQuery.getDataSource(),
          dataSourceWithPolicies
      );
    }
  }

  /**
   * Whether a datasource is a table, or a table-with-policy. Union arms built by {@link #makeUnionArm} read these.
   */
  public static boolean isTable(final DataSource dataSource)
  {
    return dataSource instanceof TableDataSource || dataSource instanceof RestrictedDataSource;
  }

  /**
   * Whether a datasource is a {@link UnionDataSource} whose arms are all tables, with or without policies.
   */
  public static boolean isUnionOfTables(final DataSource dataSource)
  {
    return dataSource instanceof UnionDataSource && dataSource.getChildren().stream().allMatch(DruidRels::isTable);
  }

  /**
   * Returns a {@link SourceDesc} for a {@link UnionDataSource} of the given arms, each presenting its signature's
   * columns in the union's column order. Types come from {@code unionRowType}, rather than from any one arm.
   */
  public static SourceDesc makeUnionSourceDesc(final List<SourceDesc> arms, final RelDataType unionRowType)
  {
    final List<String> columnNames = computeUnionColumnNames(
        arms.stream().map(arm -> arm.rowSignature.getColumnNames()).collect(Collectors.toList()),
        arms.stream().map(arm -> arm.dataSource.getReservedColumns()).collect(Collectors.toList())
    );

    final List<DataSource> dataSources = new ArrayList<>();
    for (final SourceDesc arm : arms) {
      dataSources.add(createMappingDataSource(arm.dataSource, arm.rowSignature, columnNames));
    }

    return new SourceDesc(new UnionDataSource(dataSources), RowSignatures.fromRelDataType(columnNames, unionRowType));
  }

  /**
   * Returns a datasource that also presents each column of {@code baseSignature} under the name at the same position
   * in {@code outputColumns}, using identifier {@link ExpressionVirtualColumn}s. Merges into {@code base} if it is
   * already a {@link ProjectDataSource}, and returns {@code base} if no name changes.
   * <p>
   * Throws if a virtual column of the result reads one of the new names, since it would then read the wrong column.
   */
  public static DataSource createMappingDataSource(
      final DataSource base,
      final RowSignature baseSignature,
      final List<String> outputColumns
  )
  {
    if (outputColumns.size() != baseSignature.size()) {
      throw DruidException.defensive(
          "baseSignature[%s] and outputColumns[%s] must be the same length",
          baseSignature,
          outputColumns
      );
    }

    final Set<String> renamed = new HashSet<>();
    for (int i = 0; i < outputColumns.size(); i++) {
      if (!outputColumns.get(i).equals(baseSignature.getColumnName(i))) {
        renamed.add(outputColumns.get(i));
      }
    }
    if (renamed.isEmpty()) {
      return base;
    }

    final DataSource newBase;
    final List<VirtualColumn> newVirtualColumns = new ArrayList<>();
    if (base instanceof ProjectDataSource inner) {
      newBase = inner.getBase();
      newVirtualColumns.addAll(Arrays.asList(inner.getVirtualColumns().getVirtualColumns()));
    } else {
      newBase = base;
    }

    for (int i = 0; i < outputColumns.size(); i++) {
      final String outputColumn = outputColumns.get(i);
      final String baseColumn = baseSignature.getColumnName(i);
      if (!outputColumn.equals(baseColumn)) {
        newVirtualColumns.add(
            new ExpressionVirtualColumn(
                outputColumn,
                Parser.identifier(baseColumn),
                baseSignature.getColumnType(i).orElse(null)
            )
        );
      }
    }

    for (final VirtualColumn virtualColumn : newVirtualColumns) {
      for (final String column : virtualColumn.requiredColumns()) {
        if (renamed.contains(column)) {
          throw DruidException.defensive(
              "Cannot present a column as[%s], since virtual column[%s] reads a different column of that name",
              column,
              virtualColumn.getOutputName()
          );
        }
      }
    }

    return new ProjectDataSource(newBase, VirtualColumns.create(newVirtualColumns));
  }

  /**
   * Returns the signature of the datasource of a {@link DruidRel}.
   *
   * This is not the signature of the {@link DruidRel} itself: in particular, it ignores any operations that are layered
   * on top of the datasource.
   */
  public static RowSignature dataSourceSignature(final DruidRel<?> druidRel)
  {
    if (druidRel instanceof DruidQueryRel) {
      // Get signature directly from the table.
      return ((DruidQueryRel) druidRel).getDruidTable().getRowSignature();
    } else {
      // Build the query with a no-op PartialDruidQuery.
      return druidRel.withPartialQuery(
          PartialDruidQuery.create(druidRel.getPartialDruidQuery().getScan())
      ).toDruidQuery(false).getOutputRowSignature();
    }
  }

  /**
   * Returns the names of the columns offered by a {@link UnionDataSource}. Used as an input to
   * {@link #makeUnionSourceDesc(List, RelDataType)}.
   *
   * <p>Each column takes its name from the first arm, if the other arms can present it under that name without
   * hiding anything they use. Otherwise, a new name is assigned.
   *
   * @param armColumnNames   names offered by each arm, in order. All lists must be the same length, as required by
   *                         SQL UNION semantics
   * @param armReservedNames {@link DataSource#getReservedColumns()} from each arm
   */
  static List<String> computeUnionColumnNames(
      final List<List<String>> armColumnNames,
      final List<Set<String>> armReservedNames
  )
  {
    final List<Set<String>> armUsedNames = new ArrayList<>();
    final Set<String> allNames = new HashSet<>();
    for (int arm = 0; arm < armColumnNames.size(); arm++) {
      final Set<String> usedNames = new HashSet<>(armColumnNames.get(arm));
      usedNames.addAll(armReservedNames.get(arm));
      usedNames.add(ColumnHolder.TIME_COLUMN_NAME);
      armUsedNames.add(usedNames);
      allNames.addAll(usedNames);
    }

    final String freshPrefix = Calcites.findUnusedPrefixForDigits("u", allNames);

    final List<String> names = new ArrayList<>();
    for (int i = 0; i < armColumnNames.get(0).size(); i++) {
      final String firstArmName = armColumnNames.get(0).get(i);
      boolean allArmsAgree = true;
      boolean firstArmNameFree = true;
      for (int arm = 1; arm < armColumnNames.size(); arm++) {
        final String armName = armColumnNames.get(arm).get(i);
        allArmsAgree = allArmsAgree && armName.equals(firstArmName);
        firstArmNameFree = firstArmNameFree
                           && (armName.equals(firstArmName) || !armUsedNames.get(arm).contains(firstArmName));
      }

      // A name the first arm repeats goes to only one column unless all arms agree, since the other arms may
      // present different columns at its other positions.
      if (allArmsAgree || (firstArmNameFree && !names.contains(firstArmName))) {
        names.add(firstArmName);
      } else {
        names.add(freshPrefix + i);
      }
    }

    return names;
  }
}
