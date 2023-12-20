package org.apache.druid.data.input.impl.delta;

import com.google.common.collect.Iterators;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputStats;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.stream.IntStream;

public class DeltaInputSourceReader implements InputSourceReader
{
  private static final Logger log = new Logger(DeltaInputSourceReader.class);

  private final io.delta.kernel.utils.CloseableIterator<FilteredColumnarBatch> filteredColumnarBatchCloseableIterator;
  private final InputRowSchema inputRowSchema;

  public DeltaInputSourceReader(
      io.delta.kernel.utils.CloseableIterator<FilteredColumnarBatch> filteredColumnarBatchCloseableIterator,
      InputRowSchema inputRowSchema
  )
  {
    this.filteredColumnarBatchCloseableIterator = filteredColumnarBatchCloseableIterator;
    this.inputRowSchema = inputRowSchema;
  }

  @Override
  public CloseableIterator<InputRow> read()
  {
    return new DeltaInputSourceIterator(filteredColumnarBatchCloseableIterator, inputRowSchema);
  }

  @Override
  public CloseableIterator<InputRow> read(InputStats inputStats) throws IOException
  {
    return new DeltaInputSourceIterator(filteredColumnarBatchCloseableIterator, inputRowSchema);
  }

  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException
  {
    CloseableIterator<InputRow> inner = read();
    return new CloseableIterator<InputRowListPlusRawValues>()
    {
      @Override
      public void close() throws IOException
      {
        inner.close();
      }

      @Override
      public boolean hasNext()
      {
        return inner.hasNext();
      }

      @Override
      public InputRowListPlusRawValues next()
      {
        DeltaInputRow deltaInputRow = (DeltaInputRow) inner.next();
        return InputRowListPlusRawValues.of(deltaInputRow, deltaInputRow.getRawRowAsMap());
      }
    };
  }

  private static class DeltaInputSourceIterator implements CloseableIterator<InputRow>
  {
    private final io.delta.kernel.utils.CloseableIterator<FilteredColumnarBatch> filteredColumnarBatchCloseableIterator;

    private io.delta.kernel.utils.CloseableIterator<Row> currentBatch = null;
    private final InputRowSchema inputRowSchema;

    public DeltaInputSourceIterator(io.delta.kernel.utils.CloseableIterator<FilteredColumnarBatch> filteredColumnarBatchCloseableIterator,
                                    InputRowSchema inputRowSchema
    )
    {
      this.filteredColumnarBatchCloseableIterator = filteredColumnarBatchCloseableIterator;
      this.inputRowSchema = inputRowSchema;
    }

    @Override
    public boolean hasNext() {
      while (currentBatch == null || !currentBatch.hasNext()) {
        if (!filteredColumnarBatchCloseableIterator.hasNext()) {
          return false; // No more batches or records to read!
        }
        currentBatch = filteredColumnarBatchCloseableIterator.next().getRows();
      }
      return true;
    }

    @Override
    public InputRow next()
    {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      Row dataRow = currentBatch.next();
      int numCols = dataRow.getSchema().length();

      Object[] rowValues = IntStream.range(0, numCols)
                                    .mapToObj(colOrdinal -> getValue(dataRow, colOrdinal))
                                    .toArray();

      // TODO: construct schema? remove this after debugging
      for (Object rowValue : rowValues) {
        log.info("RowValue[%s]", rowValue);
      }
      return new DeltaInputRow(dataRow, inputRowSchema);
    }

    @Override
    public void close() throws IOException
    {
      filteredColumnarBatchCloseableIterator.close();
    }
  }

  /**
   * Derive value using the type mappings from row's schema pertaining to an ordinal
   */
  private static String getValue(Row dataRow, int columnOrdinal) {
    DataType dataType = dataRow.getSchema().at(columnOrdinal).getDataType();
    if (dataRow.isNullAt(columnOrdinal)) {
      return null;
    } else if (dataType instanceof BooleanType) {
      return Boolean.toString(dataRow.getBoolean(columnOrdinal));
    } else if (dataType instanceof ByteType) {
      return Byte.toString(dataRow.getByte(columnOrdinal));
    } else if (dataType instanceof ShortType) {
      return Short.toString(dataRow.getShort(columnOrdinal));
    } else if (dataType instanceof IntegerType) {
      return Integer.toString(dataRow.getInt(columnOrdinal));
    } else if (dataType instanceof DateType) {
      // DateType data is stored internally as the number of days since 1970-01-01
      int daysSinceEpochUTC = dataRow.getInt(columnOrdinal);
      return LocalDate.ofEpochDay(daysSinceEpochUTC).toString();
    } else if (dataType instanceof LongType) {
      return Long.toString(dataRow.getLong(columnOrdinal));
    } else if (dataType instanceof TimestampType) {
      // TimestampType data is stored internally as the number of microseconds since epoch
      long microSecsSinceEpochUTC = dataRow.getLong(columnOrdinal);
      LocalDateTime dateTime = LocalDateTime.ofEpochSecond(
          microSecsSinceEpochUTC / 1_000_000 /* epochSecond */,
          (int) (1000 * microSecsSinceEpochUTC % 1_000_000) /* nanoOfSecond */,
          ZoneOffset.UTC);
      return dateTime.toString();
    } else if (dataType instanceof FloatType) {
      return Float.toString(dataRow.getFloat(columnOrdinal));
    } else if (dataType instanceof DoubleType) {
      return Double.toString(dataRow.getDouble(columnOrdinal));
    } else if (dataType instanceof StringType) {
      return dataRow.getString(columnOrdinal);
    } else if (dataType instanceof BinaryType) {
      return new String(dataRow.getBinary(columnOrdinal));
    } else if (dataType instanceof DecimalType) {
      return dataRow.getDecimal(columnOrdinal).toString();
    } else if (dataType instanceof StructType) {
      return "TODO: struct value";
    } else if (dataType instanceof ArrayType) {
      return "TODO: list value";
    } else if (dataType instanceof MapType) {
      return "TODO: map value";
    } else {
      throw new UnsupportedOperationException("unsupported data type: " + dataType);
    }
  }
}
