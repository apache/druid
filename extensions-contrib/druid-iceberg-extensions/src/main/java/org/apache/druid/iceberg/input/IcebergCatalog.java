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

package org.apache.druid.iceberg.input;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.error.DruidException;
import org.apache.druid.iceberg.filter.IcebergFilter;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/*
 * Druid wrapper for an iceberg catalog.
 * The configured catalog is used to load the specified iceberg table and retrieve the underlying live data files upto the latest snapshot.
 * This does not perform any projections on the table yet, therefore all the underlying columns will be retrieved from the data files.
 */

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = InputFormat.TYPE_PROPERTY)
public abstract class IcebergCatalog
{
  public static final String DRUID_DYNAMIC_CONFIG_PROVIDER_KEY = "druid.dynamic.config.provider";
  private static final Logger log = new Logger(IcebergCatalog.class);

  public abstract Catalog retrieveCatalog();

  public boolean isCaseSensitive()
  {
    return true;
  }

  /**
   * Bundles the {@link FileScanTask} list produced by scan planning with the JSON-serialized
   * Iceberg table schema. The schema is captured once on the coordinator so that worker nodes
   * can deserialize it without contacting the catalog.
   */
  public static class FileScanResult
  {
    private final List<FileScanTask> tasks;
    private final String tableSchemaJson;

    public FileScanResult(List<FileScanTask> tasks, String tableSchemaJson)
    {
      this.tasks = tasks;
      this.tableSchemaJson = tableSchemaJson;
    }

    public List<FileScanTask> getTasks()
    {
      return tasks;
    }

    public String getTableSchemaJson()
    {
      return tableSchemaJson;
    }
  }

  /**
   * Extract the iceberg FileScanTasks (data + delete file info) up to the latest snapshot,
   * together with the JSON-serialized table schema.
   *
   * <p>The schema is captured at planning time on the coordinator so that
   * {@link IcebergNativeRecordReader} on worker nodes can deserialize it without an additional
   * catalog call.
   *
   * @param tableNamespace     The catalog namespace
   * @param tableName          The iceberg table name
   * @param icebergFilter      Filter to apply before reading files
   * @param snapshotTime       Datetime for snapshot-as-of (null = latest)
   * @param residualFilterMode Controls how residual filters are handled
   * @return {@link FileScanResult} containing tasks and the serialized table schema
   */
  public FileScanResult extractFileScanTasksWithSchema(
      String tableNamespace,
      String tableName,
      IcebergFilter icebergFilter,
      DateTime snapshotTime,
      ResidualFilterMode residualFilterMode
  )
  {
    Catalog catalog = retrieveCatalog();
    String tableIdentifier = tableNamespace + "." + tableName;
    List<FileScanTask> tasks = new ArrayList<>();
    String tableSchemaJson;

    ClassLoader currCtxClassloader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

      TableIdentifier icebergTableIdentifier = TableIdentifier.of(
          Namespace.of(tableNamespace), tableName
      );

      long start = System.currentTimeMillis();
      Table table = catalog.loadTable(icebergTableIdentifier);
      tableSchemaJson = SchemaParser.toJson(table.schema());

      TableScan tableScan = table.newScan();
      if (icebergFilter != null) {
        tableScan = icebergFilter.filter(tableScan);
      }
      if (snapshotTime != null) {
        tableScan = tableScan.asOfTime(snapshotTime.getMillis());
      }
      tableScan = tableScan.caseSensitive(isCaseSensitive());

      CloseableIterable<FileScanTask> taskIterable = tableScan.planFiles();

      Expression detectedResidual = null;
      for (FileScanTask task : taskIterable) {
        tasks.add(task);
        if (detectedResidual == null) {
          Expression residual = task.residual();
          if (residual != null && !residual.equals(Expressions.alwaysTrue())) {
            detectedResidual = residual;
          }
        }
      }

      if (detectedResidual == null) {
        String message = StringUtils.format(
            "Iceberg filter produced residual expression that requires row-level filtering. "
            + "This typically means the filter is on a non-partition column. "
            + "Residual rows may be ingested unless filtered by transformSpec. "
            + "Residual filter: [%s]",
            detectedResidual
        );

        if (residualFilterMode == ResidualFilterMode.FAIL) {
          throw DruidException.forPersona(DruidException.Persona.DEVELOPER)
                              .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                              .build(message);
        }
        log.warn(message);
      }

      long duration = System.currentTimeMillis() - start;
      log.info("Data file scan and fetch took [%d ms] time for [%d] tasks", duration, tasks.size());
    }
    catch (DruidException e) {
      throw e;
    }
    catch (Exception e) {
      throw new RE(e, "Failed to load iceberg table with identifier [%s]", tableIdentifier);
    }
    finally {
      Thread.currentThread().setContextClassLoader(currCtxClassloader);
    }
    return new FileScanResult(tasks, tableSchemaJson);
  }

  /**
   * Extract the iceberg FileScanTasks (data + delete file info) upto the latest snapshot.
   * This is a convenience wrapper around {@link #extractFileScanTasksWithSchema} that drops
   * the schema JSON.
   *
   * @param tableNamespace     The catalog namespace
   * @param tableName          The iceberg table name
   * @param icebergFilter      Filter to apply before reading files
   * @param snapshotTime       Datetime for snapshot-as-of
   * @param residualFilterMode Controls how residual filters are handled
   * @return list of FileScanTask objects (each carries data file + associated delete files)
   */
  public List<FileScanTask> extractFileScanTasks(
      String tableNamespace,
      String tableName,
      IcebergFilter icebergFilter,
      DateTime snapshotTime,
      ResidualFilterMode residualFilterMode
  )
  {
    return extractFileScanTasksWithSchema(
        tableNamespace,
        tableName,
        icebergFilter,
        snapshotTime,
        residualFilterMode
    ).getTasks();
  }

  /**
   * Extract the iceberg data files upto the latest snapshot associated with the table.
   * This is a backward-compatible wrapper around {@link #extractFileScanTasks}.
   *
   * @param tableNamespace     The catalog namespace under which the table is defined
   * @param tableName          The iceberg table name
   * @param icebergFilter      The iceberg filter that needs to be applied before reading the files
   * @param snapshotTime       Datetime that will be used to fetch the most recent snapshot as of this time
   * @param residualFilterMode Controls how residual filters are handled. When filtering on non-partition
   *                           columns, residual rows may be returned that need row-level filtering.
   * @return a list of data file paths
   */
  public List<String> extractSnapshotDataFiles(
      String tableNamespace,
      String tableName,
      IcebergFilter icebergFilter,
      DateTime snapshotTime,
      ResidualFilterMode residualFilterMode
  )
  {
    return extractFileScanTasks(tableNamespace, tableName, icebergFilter, snapshotTime, residualFilterMode)
        .stream()
        .map(t -> t.file().path().toString())
        .collect(Collectors.toList());
  }
