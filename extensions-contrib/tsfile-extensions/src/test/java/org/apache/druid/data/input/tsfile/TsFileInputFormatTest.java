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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.FileEntity;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.error.DruidException;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.joda.time.Interval;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TsFileInputFormatTest
{
  @TempDir
  public File temporaryDirectory;

  private File sampleFile;

  @BeforeEach
  public void setUp() throws IOException, WriteProcessException
  {
    sampleFile = new File(temporaryDirectory, "sample.tsfile");
    TsFileTestData.writeSampleFile(sampleFile);
  }

  @Test
  public void testReadAllColumns() throws IOException
  {
    final InputRowSchema rowSchema = buildRowSchema(TsFileInputFormat.TIME_COLUMN_NAME, "millis");
    final List<InputRow> rows = read(new TsFileInputFormat(null, null, null), rowSchema);

    assertEquals(3, rows.size());

    final InputRow first = rows.get(0);
    assertEquals(1000L, first.getTimestampFromEpoch());
    assertEquals(Collections.singletonList("beijing"), first.getDimension("region"));
    assertEquals(Collections.singletonList("d1"), first.getDimension("device"));
    assertEquals(21.5f, (Float) first.getRaw("temperature"), 0.0001f);
    assertEquals(0.5d, (Double) first.getRaw("humidity"), 0.0001d);
    assertEquals(Boolean.TRUE, first.getRaw("status"));

    // humidity is missing for the second row.
    final InputRow second = rows.get(1);
    assertEquals(2000L, second.getTimestampFromEpoch());
    assertNull(second.getRaw("humidity"));
  }

  @Test
  public void testReadWithDefaultTimestampColumn() throws IOException
  {
    final InputRowSchema rowSchema = buildRowSchema("timestamp", "auto");
    final List<InputRow> rows = read(new TsFileInputFormat(null, null, null), rowSchema);

    assertEquals(3, rows.size());
    assertEquals(1000L, rows.get(0).getTimestampFromEpoch());
  }

  @Test
  public void testReadSelectedTableAndColumns() throws IOException
  {
    final InputRowSchema rowSchema = buildRowSchema(TsFileInputFormat.TIME_COLUMN_NAME, "millis");
    final List<InputRow> rows = read(
        new TsFileInputFormat(
            Collections.singletonList(TsFileTestData.TABLE_NAME),
            Arrays.asList("region", "temperature"),
            null
        ),
        rowSchema
    );

    assertEquals(3, rows.size());
    final InputRow first = rows.get(0);
    assertEquals(Collections.singletonList("beijing"), first.getDimension("region"));
    assertNull(first.getRaw("device"));
    assertEquals(21.5f, (Float) first.getRaw("temperature"), 0.0001f);
    assertNull(first.getRaw("humidity"));
  }

  @Test
  public void testUnknownTable()
  {
    final InputRowSchema rowSchema = buildRowSchema(TsFileInputFormat.TIME_COLUMN_NAME, "millis");
    final DruidException e = assertThrows(
        DruidException.class,
        () -> read(new TsFileInputFormat(Collections.singletonList("unknown"), null, null), rowSchema)
    );
    assertTrue(e.getMessage().contains("unknown"));
  }

  @Test
  public void testUnknownColumn()
  {
    final InputRowSchema rowSchema = buildRowSchema(TsFileInputFormat.TIME_COLUMN_NAME, "millis");
    final DruidException e = assertThrows(
        DruidException.class,
        () -> read(new TsFileInputFormat(null, Collections.singletonList("unknown"), null), rowSchema)
    );
    assertTrue(e.getMessage().contains("unknown"));
  }

  @Test
  public void testIntervalPushdown() throws IOException
  {
    final InputRowSchema rowSchema = buildRowSchema(TsFileInputFormat.TIME_COLUMN_NAME, "millis");
    final List<InputRow> rows = read(
        new TsFileInputFormat(null, null, millis(1500L, 3501L)),
        rowSchema
    );

    assertEquals(2, rows.size());
    assertEquals(2000L, rows.get(0).getTimestampFromEpoch());
    assertEquals(3000L, rows.get(1).getTimestampFromEpoch());
  }

  @Test
  public void testIntervalIsEndExclusive() throws IOException
  {
    final InputRowSchema rowSchema = buildRowSchema(TsFileInputFormat.TIME_COLUMN_NAME, "millis");
    final List<InputRow> rows = read(
        new TsFileInputFormat(null, null, millis(1000L, 2000L)),
        rowSchema
    );

    assertEquals(1, rows.size());
    assertEquals(1000L, rows.get(0).getTimestampFromEpoch());
  }

  @Test
  public void testEmptyIntervalReadsNothing() throws IOException
  {
    final InputRowSchema rowSchema = buildRowSchema(TsFileInputFormat.TIME_COLUMN_NAME, "millis");
    final List<InputRow> rows = read(
        new TsFileInputFormat(null, null, millis(2000L, 2000L)),
        rowSchema
    );

    assertTrue(rows.isEmpty());
  }

  @Test
  public void testTreeModelFileIsRejected() throws IOException, WriteProcessException
  {
    final File file = new File(temporaryDirectory, "tree-model.tsfile");
    TsFileTestData.writeTreeModelFile(file);

    final DruidException e = assertThrows(
        DruidException.class,
        () -> read(
            file,
            new TsFileInputFormat(null, null, null),
            buildRowSchema(TsFileInputFormat.TIME_COLUMN_NAME, "millis")
        )
    );
    assertTrue(e.getMessage().contains("no table schema"));
  }

  @Test
  public void testTimestampAliasShadowsSameNamedColumn() throws IOException, WriteProcessException
  {
    final File file = new File(temporaryDirectory, "timestamp-column.tsfile");
    TsFileTestData.writeFileWithTimestampColumn(file);

    final List<InputRow> rows = read(
        file,
        new TsFileInputFormat(null, null, null),
        buildRowSchema("timestamp", "millis")
    );

    assertEquals(1, rows.size());
    // The TsFile time wins over the value of the TsFile column that happens to be named "timestamp".
    assertEquals(1000L, rows.get(0).getTimestampFromEpoch());
  }

  @Test
  public void testProjectionFromColumnsFilter() throws IOException
  {
    final InputRowSchema rowSchema = new InputRowSchema(
        new TimestampSpec(TsFileInputFormat.TIME_COLUMN_NAME, "millis", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(Arrays.asList("region", "device"))),
        ColumnsFilter.inclusionBased(new HashSet<>(Arrays.asList("region", "temperature")))
    );
    final List<InputRow> rows = read(new TsFileInputFormat(null, null, null), rowSchema);

    assertEquals(3, rows.size());
    final InputRow first = rows.get(0);
    assertEquals(Collections.singletonList("beijing"), first.getDimension("region"));
    assertEquals(21.5f, (Float) first.getRaw("temperature"), 0.0001f);
    assertNull(first.getRaw("device"));
    assertNull(first.getRaw("status"));
  }

  @Test
  public void testProjectionFromFixedDimensions() throws IOException
  {
    // MSQ EXTERN passes ColumnsFilter.all() and puts the external table's column signature in the dimensions.
    final InputRowSchema rowSchema = new InputRowSchema(
        new TimestampSpec(TsFileInputFormat.TIME_COLUMN_NAME, "millis", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(Arrays.asList("region", "temperature"))),
        ColumnsFilter.all()
    );
    final List<InputRow> rows = read(new TsFileInputFormat(null, null, null), rowSchema);

    assertEquals(3, rows.size());
    assertEquals(Collections.singletonList("beijing"), rows.get(0).getDimension("region"));
    assertEquals(21.5f, (Float) rows.get(0).getRaw("temperature"), 0.0001f);
    assertNull(rows.get(0).getRaw("device"));
    assertNull(rows.get(0).getRaw("humidity"));
    assertNull(rows.get(0).getRaw("status"));
  }

  @Test
  public void testTagOnlyProjectionFallsBackToAllColumns() throws IOException
  {
    // TsFile's table reader returns no rows at all when only tag columns are selected.
    final InputRowSchema rowSchema = new InputRowSchema(
        new TimestampSpec(TsFileInputFormat.TIME_COLUMN_NAME, "millis", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(Collections.singletonList("region"))),
        ColumnsFilter.all()
    );
    final List<InputRow> rows = read(new TsFileInputFormat(null, null, null), rowSchema);

    assertEquals(3, rows.size());
    assertEquals(Collections.singletonList("beijing"), rows.get(0).getDimension("region"));
    assertEquals(21.5f, (Float) rows.get(0).getRaw("temperature"), 0.0001f);
  }

  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    new TsFileExtensionsModule().getJacksonModules().forEach(mapper::registerModule);

    final String json = "{\"type\":\"tsfile\",\"tables\":[\"sensor\"],\"columns\":[\"region\",\"temperature\"],"
                        + "\"interval\":\"1970-01-01T00:00:00.000Z/1970-01-01T00:00:04.000Z\"}";
    final TsFileInputFormat format = (TsFileInputFormat) mapper.readValue(json, InputFormat.class);
    assertEquals(Collections.singletonList(TsFileTestData.TABLE_NAME), format.getTables());
    assertEquals(Arrays.asList("region", "temperature"), format.getColumns());
    assertEquals(millis(0L, 4000L), format.getInterval());
    assertEquals(format, mapper.readValue(mapper.writeValueAsString(format), InputFormat.class));
  }

  @Test
  public void testReadCommittedSampleFile() throws IOException
  {
    final File file = new File("examples/data/sensor.tsfile");
    final List<InputRow> rows = read(
        file,
        new TsFileInputFormat(null, null, null),
        buildRowSchema(TsFileInputFormat.TIME_COLUMN_NAME, "millis")
    );

    assertEquals(TsFileTestData.TIMESTAMPS.length, rows.size());
    for (int i = 0; i < rows.size(); i++) {
      final InputRow row = rows.get(i);
      assertEquals(TsFileTestData.TIMESTAMPS[i], row.getTimestampFromEpoch());
      assertEquals(Collections.singletonList(TsFileTestData.REGIONS[i]), row.getDimension("region"));
      assertEquals(Collections.singletonList(TsFileTestData.DEVICES[i]), row.getDimension("device"));
      assertEquals(TsFileTestData.TEMPERATURES[i], (Float) row.getRaw("temperature"), 0.0001f);
      if (Double.isNaN(TsFileTestData.HUMIDITIES[i])) {
        assertNull(row.getRaw("humidity"));
      } else {
        assertEquals(TsFileTestData.HUMIDITIES[i], (Double) row.getRaw("humidity"), 0.0001d);
      }
    }
  }

  private List<InputRow> read(TsFileInputFormat format, InputRowSchema rowSchema) throws IOException
  {
    return read(sampleFile, format, rowSchema);
  }

  private List<InputRow> read(File file, TsFileInputFormat format, InputRowSchema rowSchema) throws IOException
  {
    final InputEntityReader reader = format.createReader(rowSchema, new FileEntity(file), temporaryDirectory);
    final List<InputRow> rows = new ArrayList<>();
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      while (iterator.hasNext()) {
        rows.add(iterator.next());
      }
    }
    return rows;
  }

  private static Interval millis(long startMillis, long endMillis)
  {
    return new Interval(DateTimes.utc(startMillis), DateTimes.utc(endMillis));
  }

  /**
   * Schema used by most tests: no column projection is applied, so the TsFile columns are read in full.
   */
  private static InputRowSchema buildRowSchema(String timestampColumn, String timestampFormat)
  {
    return new InputRowSchema(
        new TimestampSpec(timestampColumn, timestampFormat, null),
        DimensionsSpec.builder()
                      .setDimensions(DimensionsSpec.getDefaultSchemas(Arrays.asList("region", "device")))
                      .useSchemaDiscovery(true)
                      .build(),
        ColumnsFilter.all()
    );
  }
}
