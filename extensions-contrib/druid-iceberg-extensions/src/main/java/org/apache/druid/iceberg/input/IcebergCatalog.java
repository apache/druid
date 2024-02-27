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
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.joda.time.DateTime;

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
  private static final Logger log = new Logger(IcebergCatalog.class);

  public abstract BaseMetastoreCatalog retrieveCatalog();

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
      CloseableIterable<FileScanTask> tasks = tableScan.planFiles();
      CloseableIterable.transform(tasks, FileScanTask::file)
                       .forEach(dataFile -> dataFilePaths.add(dataFile.path().toString()));

      long duration = System.currentTimeMillis() - start;
      log.info("Data file scan and fetch took [%d ms] time for [%d] paths", duration, dataFilePaths.size());
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
