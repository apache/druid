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
import org.apache.druid.iceberg.filter.IcebergFilter;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
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
   * @param tableNamespace The catalog namespace under which the table is defined
   * @param tableName      The iceberg table name
   * @param icebergFilter      The iceberg filter that needs to be applied before reading the files
   * @param snapshotTime      Datetime that will be used to fetch the most recent snapshot as of this time
   * @return a list of data file paths
   */
  public List<String> extractSnapshotDataFiles(
      String tableNamespace,
      String tableName,
      IcebergFilter icebergFilter,
      DateTime snapshotTime
  )
  {
    return extractSnapshotDataFiles(tableNamespace, tableName, icebergFilter, snapshotTime, null);
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
      @Nullable ResidualFilterMode residualFilterMode
  )
  {
    Catalog catalog = retrieveCatalog();
    Namespace namespace = Namespace.of(tableNamespace);
    String tableIdentifier = tableNamespace + "." + tableName;

    List<String> dataFilePaths = new ArrayList<>();
    ResidualFilterMode effectiveMode = residualFilterMode != null ? residualFilterMode : ResidualFilterMode.IGNORE;

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
        dataFilePaths.add(task.file().path().toString());

        // Check for residual filters if mode is not IGNORE
        if (effectiveMode != ResidualFilterMode.IGNORE && detectedResidual == null) {
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

        if (effectiveMode == ResidualFilterMode.FAIL) {
          throw new IAE(
              "%s To allow residual rows, set residualFilterMode to 'ignore' or 'warn', "
              + "or add a corresponding filter in transformSpec.",
              message
          );
        } else if (effectiveMode == ResidualFilterMode.WARN) {
          log.warn(message);
        }
      }

      long duration = System.currentTimeMillis() - start;
      log.info("Data file scan and fetch took [%d ms] time for [%d] paths", duration, dataFilePaths.size());
    }
    catch (IAE e) {
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
}
