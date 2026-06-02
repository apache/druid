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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.iceberg.filter.IcebergFilter;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.File;

public class IcebergArrowInputSource extends AbstractIcebergInputSource implements InputSource
{
  public static final String TYPE_KEY = "iceberg_arrow";

  @JsonProperty
  private final int arrowBatchSize;

  @JsonCreator
  public IcebergArrowInputSource(
      @JsonProperty("tableName") String tableName,
      @JsonProperty("namespace") String namespace,
      @JsonProperty("icebergFilter") @Nullable IcebergFilter icebergFilter,
      @JsonProperty("icebergCatalog") IcebergCatalog icebergCatalog,
      @JsonProperty("snapshotTime") @Nullable DateTime snapshotTime,
      @JsonProperty("residualFilterMode") @Nullable ResidualFilterMode residualFilterMode,
      @JsonProperty("arrowBatchSize") @Nullable Integer arrowBatchSize
  )
  {
    super(tableName, namespace, icebergFilter, icebergCatalog, snapshotTime, residualFilterMode);
    this.arrowBatchSize = arrowBatchSize != null && arrowBatchSize > 0
                          ? arrowBatchSize
                          : IcebergArrowInputSourceReader.DEFAULT_BATCH_SIZE;
  }

  @JsonProperty
  public int getArrowBatchSize()
  {
    return arrowBatchSize;
  }

  @Override
  public boolean needsFormat()
  {
    return false;
  }

  @Override
  public boolean isSplittable()
  {
    return false;
  }

  @Override
  public InputSourceReader reader(
      InputRowSchema inputRowSchema,
      @Nullable InputFormat inputFormat,
      File temporaryDirectory
  )
  {
    final Table table = retrieveTable();
    if (icebergFilter != null) {
      final TableScan filteredScan = icebergFilter.filter(
          table.newScan().caseSensitive(icebergCatalog.isCaseSensitive())
      );
      icebergCatalog.enforceResidualMode(filteredScan, getResidualFilterMode());
    }
    return new IcebergArrowInputSourceReader(
        table,
        icebergFilter,
        snapshotTime,
        icebergCatalog.isCaseSensitive(),
        inputRowSchema,
        arrowBatchSize
    );
  }
}
