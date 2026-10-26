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

package org.apache.druid.data.input.tsfile;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.ColumnSchemaBuilder;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.v4.ITsFileWriter;
import org.apache.tsfile.write.v4.TsFileWriterBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * Writes the sample TsFile used by {@link TsFileInputFormatTest} and by the example ingestion spec in
 * {@code examples/}. Also serves as documentation for the TsFile layout the extension reads.
 */
public class TsFileTestData
{
  public static final String TABLE_NAME = "sensor";
  public static final long[] TIMESTAMPS = {1000L, 2000L, 3000L};
  public static final String[] REGIONS = {"beijing", "beijing", "shanghai"};
  public static final String[] DEVICES = {"d1", "d2", "d3"};
  public static final float[] TEMPERATURES = {21.5f, 22.5f, 23.5f};
  /** NaN marks a row where the humidity is not written, which TsFile reads back as null. */
  public static final double[] HUMIDITIES = {0.5d, Double.NaN, 0.7d};

  private TsFileTestData()
  {
  }

  /**
   * Writes {@code examples/data/sensor.tsfile}, used by the example ingestion spec. Run from this module:
   * <pre>
   * mvn exec:java -Dexec.classpathScope=test \
   *   -Dexec.mainClass=org.apache.druid.data.input.tsfile.TsFileTestData
   * </pre>
   */
  public static void main(String[] args) throws IOException, WriteProcessException
  {
    final Path outputDir = Paths.get(args.length > 0 ? args[0] : "examples/data");
    Files.createDirectories(outputDir);
    final File file = outputDir.resolve("sensor.tsfile").toFile();
    writeSampleFile(file);
    System.out.println("Wrote " + file.getAbsolutePath());
  }

  public static void writeSampleFile(File file) throws IOException, WriteProcessException
  {
    final TableSchema tableSchema = new TableSchema(
        TABLE_NAME,
        Arrays.asList(
            new ColumnSchemaBuilder().name("region").dataType(TSDataType.STRING).category(ColumnCategory.TAG).build(),
            new ColumnSchemaBuilder().name("device").dataType(TSDataType.STRING).category(ColumnCategory.TAG).build(),
            new ColumnSchemaBuilder().name("temperature").dataType(TSDataType.FLOAT).build(),
            new ColumnSchemaBuilder().name("humidity").dataType(TSDataType.DOUBLE).build(),
            new ColumnSchemaBuilder().name("status").dataType(TSDataType.BOOLEAN).build()
        )
    );

    final Tablet tablet = new Tablet(
        Arrays.asList("region", "device", "temperature", "humidity", "status"),
        Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.BOOLEAN)
    );

    for (int i = 0; i < TIMESTAMPS.length; i++) {
      tablet.addTimestamp(i, TIMESTAMPS[i]);
      tablet.addValue(i, "region", REGIONS[i]);
      tablet.addValue(i, "device", DEVICES[i]);
      tablet.addValue(i, "temperature", TEMPERATURES[i]);
      tablet.addValue(i, "status", i % 2 == 0);
      if (!Double.isNaN(HUMIDITIES[i])) {
        tablet.addValue(i, "humidity", HUMIDITIES[i]);
      }
    }

    try (ITsFileWriter writer = new TsFileWriterBuilder().file(file).tableSchema(tableSchema).build()) {
      writer.write(tablet);
    }
  }
}
