package org.apache.druid.delta.input;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.delta.kernel.Scan;
import io.delta.kernel.ScanBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.TableNotFoundException;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.client.DefaultTableClient;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class DeltaInputRowTest
{
  private static final String DELTA_TABLE_PATH = "src/test/resources/people-delta-table";
  private static final List<String> DIMENSIONS = ImmutableList.of("city", "state", "surname", "email", "country");
  private static final List<Map<String, Object>> EXPECTED_ROWS = new ArrayList<>(
      ImmutableList.of(
          ImmutableMap.of(
              "birthday", 1049418130358332L,
              "country", "Panama",
              "city", "Eastpointe",
              "surname", "Francis",
              "name", "Darren",
              "state", "Minnesota",
              "email", "rating1998@yandex.com"
          ),
          ImmutableMap.of(
              "birthday", 1283743763753323L,
              "country", "Aruba",
              "city", "Wheaton",
              "surname", "Berger",
              "name", "Madelene",
              "state", "New York",
              "email", "invitations2036@duck.com"
          ),
          ImmutableMap.of(
              "birthday", 1013053015543401L,
              "country", "Anguilla",
              "city", "Sahuarita",
              "surname", "Mccall",
              "name", "Anibal",
              "state", "Oklahoma",
              "email", "modifications2025@yahoo.com"
          ),
          ImmutableMap.of(
              "birthday", 569564422313618L,
              "country", "Luxembourg",
              "city", "Santa Rosa",
              "surname", "Jackson",
              "name", "Anibal",
              "state", "New Hampshire",
              "email", "medication1855@gmail.com"
          ),
          ImmutableMap.of(
              "birthday", 667560498632507L,
              "country", "Anguilla",
              "city", "Morristown",
              "surname", "Tanner",
              "name", "Loree",
              "state", "New Hampshire",
              "email", "transport1961@duck.com"
          ),
          ImmutableMap.of(
              "birthday", 826120534655077L,
              "country", "Panama",
              "city", "Greenville",
              "surname", "Gamble",
              "name", "Bernardo",
              "state", "North Carolina",
              "email", "limitations1886@yandex.com"
          ),
          ImmutableMap.of(
              "birthday", 1284652116668688L,
              "country", "China",
              "city", "Albert Lea",
              "surname", "Cherry",
              "name", "Philip",
              "state", "Nevada",
              "email", "const1874@outlook.com"
          ),
          ImmutableMap.of(
              "birthday", 1154549284242934L,
              "country", "Barbados",
              "city", "Mount Pleasant",
              "surname", "Beasley",
              "name", "Shaneka",
              "state", "Montana",
              "email", "msg1894@example.com"
          ),
          ImmutableMap.of(
              "birthday", 1034695930678172L,
              "country", "Honduras",
              "city", "Hutchinson",
              "surname", "Vinson",
              "name", "Keneth",
              "state", "Connecticut",
              "email", "questions2074@gmail.com"
          ),
          ImmutableMap.of(
              "birthday", 1166606855236945L,
              "country", "Senegal",
              "city", "Galt",
              "surname", "Schwartz",
              "name", "Hee",
              "state", "New Jersey",
              "email", "statements2016@protonmail.com"
          )
      )
  );

  @Test
  public void testSerializeDeserializeRoundtrip() throws TableNotFoundException, IOException
  {
    TableClient tableClient = DefaultTableClient.create(new Configuration());
    Table table = Table.forPath(tableClient, DELTA_TABLE_PATH);
    Snapshot snapshot = table.getLatestSnapshot(tableClient);
    StructType readSchema = snapshot.getSchema(tableClient);
    ScanBuilder scanBuilder = snapshot.getScanBuilder(tableClient)
                                      .withReadSchema(tableClient, readSchema);
    Scan scan = scanBuilder.build();
    Row scanState = scan.getScanState(tableClient);

    InputRowSchema schema = new InputRowSchema(
        new TimestampSpec("birthday", "auto", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(DIMENSIONS)),
        ColumnsFilter.all()
    );

    CloseableIterator<FilteredColumnarBatch> scanFileIter = scan.getScanFiles(tableClient);
    int totalRecordCount = 0;
    while (scanFileIter.hasNext()) {
      try (CloseableIterator<FilteredColumnarBatch> data =
               Scan.readData(
                   tableClient,
                   scanState,
                   scanFileIter.next().getRows(),
                   Optional.empty()
               )) {
        while (data.hasNext()) {
          FilteredColumnarBatch dataReadResult = data.next();
          Row next = dataReadResult.getRows().next();
          DeltaInputRow deltaInputRow = new DeltaInputRow(
              next,
              schema
          );
          Assert.assertNotNull(deltaInputRow);
          Assert.assertEquals(DIMENSIONS, deltaInputRow.getDimensions());

          Map<String, Object> expectedRow = EXPECTED_ROWS.get(totalRecordCount);
          Assert.assertEquals(expectedRow, deltaInputRow.getRawRowAsMap());

          for (String dimension : DIMENSIONS) {
            Assert.assertEquals(expectedRow.get(dimension), deltaInputRow.getDimension(dimension).get(0));
          }
          totalRecordCount += 1;
        }
      }
    }
    Assert.assertEquals(EXPECTED_ROWS.size(), totalRecordCount);
  }
}