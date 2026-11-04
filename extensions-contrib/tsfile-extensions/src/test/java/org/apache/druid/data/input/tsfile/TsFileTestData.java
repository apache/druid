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
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.tsfile.write.schema.MeasurementSchema;
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

  /**
   * Writes a legacy tree-model TsFile, which has no table schema in its footer and is not readable by the
   * extension.
   */
  public static void writeTreeModelFile(File file) throws IOException, WriteProcessException
  {
    try (TsFileWriter writer = new TsFileWriter(file)) {
      writer.registerTimeseries("root.sg.d1", new MeasurementSchema("s1", TSDataType.INT32));
      writer.writeRecord(new TSRecord("root.sg.d1", TIMESTAMPS[0]).addTuple(new IntDataPoint("s1", 1)));
      writer.flush();
    }
  }

  /**
   * Writes a table-model file that has a value column literally named {@code timestamp}, so that tests can check
   * the timestamp alias wins over a same-named column.
   */
  public static void writeFileWithTimestampColumn(File file) throws IOException, WriteProcessException
  {
    final TableSchema tableSchema = new TableSchema(
        TABLE_NAME,
        Arrays.asList(
            new ColumnSchemaBuilder().name("timestamp").dataType(TSDataType.INT64).build(),
            new ColumnSchemaBuilder().name("temperature").dataType(TSDataType.FLOAT).build()
        )
    );

    final Tablet tablet = new Tablet(
        Arrays.asList("timestamp", "temperature"),
        Arrays.asList(TSDataType.INT64, TSDataType.FLOAT)
    );
    tablet.addTimestamp(0, TIMESTAMPS[0]);
    tablet.addValue(0, "timestamp", 999_999L);
    tablet.addValue(0, "temperature", TEMPERATURES[0]);

    try (ITsFileWriter writer = new TsFileWriterBuilder().file(file).tableSchema(tableSchema).build()) {
      writer.write(tablet);
    }
  }
}
