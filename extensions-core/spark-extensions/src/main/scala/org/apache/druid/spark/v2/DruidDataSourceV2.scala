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

package org.apache.druid.spark.v2

import org.apache.druid.common.config.NullHandling
import org.apache.druid.spark.configuration.Configuration
import org.apache.druid.spark.v2.reader.DruidDataSourceReader
import org.apache.druid.spark.v2.writer.DruidDataSourceWriter

import java.util.Optional
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.types.StructType

class DruidDataSourceV2 extends DataSourceV2 with ReadSupport with WriteSupport
  with DataSourceRegister with Logging {
  // We don't have Guice injections for modules so we need to initialize NullHandling ourselves, same as for Druid tests
  NullHandling.initializeForTests()

  override def shortName(): String = DruidDataSourceV2ShortName

  /**
    * Create a DataSourceReader to read data in from Druid, configured via DATASOURCEOPTIONS.
    *
    * @param dataSourceOptions A wrapper around the properties map specifed via `.option` or `.options` calls on the
    *                          DataSourceReader.
    * @return A DataSourceReader capable of reading data from Druid as configured via DATASOURCEOPTIONS.
    */
  override def createReader(dataSourceOptions: DataSourceOptions): DataSourceReader = {
    DruidDataSourceReader(dataSourceOptions)
  }

  /**
    * Create a DataSourceReader to read data in from Druid, configured via DATASOURCEOPTIONS. The provided schema will
    * be used instead of making calls to the broker.
    *
    * @param dataSourceOptions A wrapper around the properties map specifed via `.option` or `.options` calls on the
    *                          DataSourceReader.
    * @param schema The schema to use when reading data. Specified via the `.schema` method of a DataSourceReader.
    * @return A DataSourceReader capable of reading data from Druid as configured via DATASOURCEOPTIONS.
    */
  override def createReader(schema: StructType,
                            dataSourceOptions: DataSourceOptions): DataSourceReader = {
    DruidDataSourceReader(schema, dataSourceOptions)
  }

  /**
    * Create a writer to save a dataframe as a Druid table with save mode SAVEMODE, configured via DATASOURCEOPTIONS.
    * SCHEMA is inferred automatically from the source dataframe. The segments written out by this writer will match the
    * partitioning of the source dataframe.
    *
    * @param uuid The unique job id assigned by Spark.
    * @param schema The schema of the dataframe to write to Druid.
    * @param saveMode The save mode to use when writing data to Druid.
    * @param dataSourceOptions A wrapper around the properties map specified via `.option` or `.options` call on the
    *                          DataSourceWriter.
    * @return A DataSourceWriter capable of writing data to Druid based on the specified schema, save mode, and
    *         data source options.
    */
  override def createWriter(uuid: String,
                            schema: StructType,
                            saveMode: SaveMode,
                            dataSourceOptions: DataSourceOptions): Optional[DataSourceWriter] = {
    // TODO: Take advantage of the job id being provided (uuid in the args list)
    DruidDataSourceWriter(schema, saveMode, Configuration(dataSourceOptions))
  }
}
