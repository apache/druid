package org.apache.druid.data.input.impl.delta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.TableNotFoundException;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.client.DefaultTableClient;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.*;

import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.data.input.impl.InputEntityIteratingReader;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.data.input.impl.systemfield.SystemFieldDecoratorFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.hadoop.conf.Configuration;


public class DeltaInputSource implements SplittableInputSource<DeltaSplit>
{
  public static final String TYPE_KEY = "delta";
  private final String tablePath;
  @Nullable
  private final DeltaSplit deltaSplit;

  @JsonCreator
  public DeltaInputSource(
      @JsonProperty("tablePath") String tablePath,
      @JsonProperty("deltaSplit") @Nullable DeltaSplit deltaSplit
  )
  {
    this.tablePath = tablePath;
    this.deltaSplit = deltaSplit;
  }

  @Override
  public boolean needsFormat()
  {
    // Only support Parquet
    return false;
  }

  @Override
  public InputSourceReader reader(
      InputRowSchema inputRowSchema,
      @Nullable InputFormat inputFormat,
      File temporaryDirectory
  )
  {
    String myTablePath = tablePath; // fully qualified table path. Ex: file:/user/tables/myTable
    Configuration hadoopConf = new Configuration();
    TableClient myTableClient = DefaultTableClient.create(hadoopConf);
    try {
      Row scanState = null;
      List<Row> scanRowList = null;
      if (deltaSplit != null) {
        scanState = deserialize(myTableClient, deltaSplit.getStateRow());
        scanRowList = deltaSplit.getFileRows().stream().map(row -> deserialize(myTableClient, row)).collect(Collectors.toList());
      } else {
        Table myTable = Table.forPath(myTableClient, myTablePath);
        Snapshot mySnapshot = myTable.getLatestSnapshot(myTableClient);
        Scan myScan = mySnapshot.getScanBuilder(myTableClient).build();
        scanState = myScan.getScanState(myTableClient);
        CloseableIterator<FilteredColumnarBatch> myScanFilesAsBatches = myScan.getScanFiles(myTableClient);
        scanRowList = new ArrayList<>();
        while (myScanFilesAsBatches.hasNext()) {
          FilteredColumnarBatch scanFileBatch = myScanFilesAsBatches.next();
          CloseableIterator<Row> myScanFilesAsRows = scanFileBatch.getRows();
          myScanFilesAsRows.forEachRemaining(scanRowList::add);
        }
      }
      return new DeltaInputSourceReader(Scan.readData(
          myTableClient,
          scanState,
          Utils.toCloseableIterator(scanRowList.iterator()),
          Optional.empty()
      ));
    }
    catch (TableNotFoundException e) {
      throw InvalidInput.exception(e, "Table not found: %s", myTablePath);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Stream<InputSplit<DeltaSplit>> createSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
      throws IOException
  {
    return null;
  }

  @Override
  public int estimateNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec) throws IOException
  {
    return 0;
  }

  @Override
  public InputSource withSplit(InputSplit<DeltaSplit> split)
  {
    return null;
  }

  private Row deserialize(TableClient myTableClient, String row)
  {
    return RowSerde.deserializeRowFromJson(myTableClient, row);
  }
}
