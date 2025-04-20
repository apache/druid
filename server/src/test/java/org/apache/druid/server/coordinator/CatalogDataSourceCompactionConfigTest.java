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

package org.apache.druid.server.coordinator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.catalog.MapMetadataCatalog;
import org.apache.druid.catalog.MetadataCatalog;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.DatasourceProjectionMetadata;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.table.DatasourceDefn;
import org.apache.druid.catalog.model.table.TableBuilder;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CatalogDataSourceCompactionConfigTest
{
  private static final String TEST_DS = "test";
  private static final String TEST_DS_PROJECTIONS_ONLY_SCHEMA = "test_projections";

  private static final ObjectMapper MAPPER;
  private static final MapMetadataCatalog METADATA_CATALOG;

  private static final AggregateProjectionSpec TEST_PROJECTION_SPEC_1 = new AggregateProjectionSpec(
      "string_sum_long_hourly",
      VirtualColumns.create(
          Granularities.toVirtualColumn(Granularities.HOUR, Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME)
      ),
      ImmutableList.of(
          new StringDimensionSchema("string")
      ),
      new AggregatorFactory[]{
          new LongSumAggregatorFactory("sum_long", "long")
      }
  );

  static {
    MAPPER = new DefaultObjectMapper();
    METADATA_CATALOG = new MapMetadataCatalog(MAPPER);
    MAPPER.setInjectableValues(
        new InjectableValues.Std()
            .addValue(MetadataCatalog.class, METADATA_CATALOG)
            .addValue(ExprMacroTable.class.getName(), TestExprMacroTable.INSTANCE)
            .addValue(ObjectMapper.class.getName(), MAPPER)
    );

    METADATA_CATALOG.addSpec(
        TableId.datasource(TEST_DS),
        TableBuilder.datasource(TEST_DS, "P1D")
                    .columns(
                        ImmutableList.of(
                            new ColumnSpec(ColumnHolder.TIME_COLUMN_NAME, ColumnType.LONG.asTypeString(), null),
                            new ColumnSpec("string", ColumnType.STRING.asTypeString(), null),
                            new ColumnSpec("double", ColumnType.DOUBLE.asTypeString(), null),
                            new ColumnSpec("long", ColumnType.LONG.asTypeString(), null)
                        )
                    )
                    .property(
                        DatasourceDefn.PROJECTIONS_KEYS_PROPERTY,
                        ImmutableList.of(
                            new DatasourceProjectionMetadata(TEST_PROJECTION_SPEC_1)
                        )
                    )
                    .buildSpec()
    );
    METADATA_CATALOG.addSpec(
        TableId.datasource(TEST_DS_PROJECTIONS_ONLY_SCHEMA),
        TableBuilder.datasource(TEST_DS_PROJECTIONS_ONLY_SCHEMA, "P1D")
                    .property(
                        DatasourceDefn.PROJECTIONS_KEYS_PROPERTY,
                        ImmutableList.of(
                            new DatasourceProjectionMetadata(TEST_PROJECTION_SPEC_1)
                        )
                    )
                    .buildSpec()

    );
  }

  @Test
  public void testProjections()
  {
    final CatalogDataSourceCompactionConfig config = new CatalogDataSourceCompactionConfig(
        TEST_DS,
        null,
        null,
        null,
        null,
        null,
        METADATA_CATALOG
    );

    Assertions.assertEquals(
        TEST_PROJECTION_SPEC_1,
        config.getProjections().get(0)
    );
  }

  @Test
  public void testSerde() throws JsonProcessingException
  {
    final CatalogDataSourceCompactionConfig config = new CatalogDataSourceCompactionConfig(
        "foo",
        null,
        null,
        null,
        null,
        null,
        METADATA_CATALOG
    );

    Assertions.assertEquals(
        config,
        MAPPER.readValue(MAPPER.writeValueAsString(config), DataSourceCompactionConfig.class)
    );
  }

  @Test
  public void testEqualsAndHashcode()
  {
    EqualsVerifier.forClass(CatalogDataSourceCompactionConfig.class)
                  .usingGetClass()
                  .withIgnoredFields("catalog", "tableId")
                  .verify();
  }
}
