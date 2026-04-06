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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
   * Extract the iceberg data files upto the latest snapshot associated with the table
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
    Catalog catalog = retrieveCatalog();
    Namespace namespace = Namespace.of(tableNamespace);
    String tableIdentifier = tableNamespace + "." + tableName;

    List<String> dataFilePaths = new ArrayList<>();

    ClassLoader currCtxClassloader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
      TableIdentifier icebergTableIdentifier = catalog.listTables(namespace).stream()
                                                      .filter(tableId -> tableId.toString().equals(tableIdentifier))
                                                      .findFirst()
                                                      .orElseThrow(() -> new IAE(
                                                          " Couldn't retrieve table identifier for '%s'. Please verify that the table exists in the given catalog",
                                                          tableIdentifier
                                                      ));

      long start = System.currentTimeMillis();
      TableScan tableScan = catalog.loadTable(icebergTableIdentifier).newScan();

      if (icebergFilter != null) {
        tableScan = icebergFilter.filter(tableScan);
      }
      if (snapshotTime != null) {
        tableScan = tableScan.asOfTime(snapshotTime.getMillis());
      }

      tableScan = tableScan.caseSensitive(isCaseSensitive());
      CloseableIterable<FileScanTask> tasks = tableScan.planFiles();

      Expression detectedResidual = null;
      for (FileScanTask task : tasks) {
        dataFilePaths.add(task.file().location());

        // Check for residual filters
        if (detectedResidual == null) {
          Expression residual = task.residual();
          if (residual != null && !residual.equals(Expressions.alwaysTrue())) {
            detectedResidual = residual;
          }
        }
      }

      // Handle residual filter based on mode
      if (detectedResidual != null) {
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
      log.info("Data file scan and fetch took [%d ms] time for [%d] paths", duration, dataFilePaths.size());
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
    return dataFilePaths;
  }

  /**
   * Result container for a file scan that preserves the Table reference and
   * individual FileScanTasks with their associated delete files.
   */
  public static class FileScanResult
  {
    private final Table table;
    private final List<FileScanTask> fileScanTasks;
    private final boolean hasDeleteFiles;

    FileScanResult(final Table table, final List<FileScanTask> fileScanTasks, final boolean hasDeleteFiles)
    {
      this.table = table;
      this.fileScanTasks = fileScanTasks;
      this.hasDeleteFiles = hasDeleteFiles;
    }

    public Table getTable()
    {
      return table;
    }

    public List<FileScanTask> getFileScanTasks()
    {
      return fileScanTasks;
    }

    public boolean hasDeleteFiles()
    {
      return hasDeleteFiles;
    }
  }

  /**
   * Scan the iceberg table and return FileScanTasks with their delete file metadata intact.
   * This is used when v2 delete handling is enabled, allowing the caller to apply deletes
   * via Iceberg's native reader stack rather than just extracting raw file paths.
   *
   * @param tableNamespace     The catalog namespace under which the table is defined
   * @param tableName          The iceberg table name
   * @param icebergFilter      The iceberg filter to apply for partition pruning
   * @param snapshotTime       Datetime for snapshot time-travel (null for latest)
   * @param residualFilterMode Controls how residual filters are handled
   * @return a FileScanResult containing the table, tasks, and delete file presence flag
   */
  public FileScanResult extractFileScanTasks(
      final String tableNamespace,
      final String tableName,
      final IcebergFilter icebergFilter,
      final DateTime snapshotTime,
      final ResidualFilterMode residualFilterMode
  )
  {
    final Catalog catalog = retrieveCatalog();
    final Namespace namespace = Namespace.of(tableNamespace);
    final String tableIdentifier = tableNamespace + "." + tableName;

    final ClassLoader currCtxClassloader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
      final TableIdentifier icebergTableIdentifier = catalog.listTables(namespace).stream()
                                                            .filter(tableId -> tableId.toString().equals(tableIdentifier))
                                                            .findFirst()
                                                            .orElseThrow(() -> new IAE(
                                                                " Couldn't retrieve table identifier for '%s'. "
                                                                + "Please verify that the table exists in the given catalog",
                                                                tableIdentifier
                                                            ));

      final long start = System.currentTimeMillis();
      final Table table = catalog.loadTable(icebergTableIdentifier);
      TableScan tableScan = table.newScan();

      if (icebergFilter != null) {
        tableScan = icebergFilter.filter(tableScan);
      }
      if (snapshotTime != null) {
        tableScan = tableScan.asOfTime(snapshotTime.getMillis());
      }
      tableScan = tableScan.caseSensitive(isCaseSensitive());

      final List<FileScanTask> fileScanTasks = new ArrayList<>();
      boolean hasDeleteFiles = false;
      Expression detectedResidual = null;

      try (CloseableIterable<FileScanTask> tasks = tableScan.planFiles()) {
        for (final FileScanTask task : tasks) {
          fileScanTasks.add(task);

          if (!hasDeleteFiles && !task.deletes().isEmpty()) {
            hasDeleteFiles = true;
          }

          if (detectedResidual == null) {
            final Expression residual = task.residual();
            if (residual != null && !residual.equals(Expressions.alwaysTrue())) {
              detectedResidual = residual;
            }
          }
        }
      }
      catch (IOException e) {
        throw new RE(e, "Failed to plan files for iceberg table [%s]", tableIdentifier);
      }

      if (detectedResidual != null) {
        final String message = StringUtils.format(
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

      final long duration = System.currentTimeMillis() - start;
      log.info(
          "File scan task extraction took [%d ms] for [%d] tasks, hasDeleteFiles=[%s]",
          duration,
          fileScanTasks.size(),
          hasDeleteFiles
      );

      return new FileScanResult(table, fileScanTasks, hasDeleteFiles);
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
  }
}
