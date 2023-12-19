package org.apache.druid.data.input.impl.delta;

import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputStats;
import org.apache.druid.java.util.common.parsers.CloseableIterator;

import java.io.IOException;

public class DeltaInputSourceReader implements InputSourceReader
{
  private final io.delta.kernel.utils.CloseableIterator<FilteredColumnarBatch> filteredColumnarBatchCloseableIterator;

  public DeltaInputSourceReader(io.delta.kernel.utils.CloseableIterator<FilteredColumnarBatch> filteredColumnarBatchCloseableIterator)
  {

    this.filteredColumnarBatchCloseableIterator = filteredColumnarBatchCloseableIterator;
  }

  @Override
  public CloseableIterator<InputRow> read(InputStats inputStats) throws IOException
  {
    return null;
  }

  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException
  {
    return null;
  }

  private static class DeltaInputSourceIterator implements CloseableIterator<InputRow>
  {
    private final io.delta.kernel.utils.CloseableIterator<FilteredColumnarBatch> filteredColumnarBatchCloseableIterator;
    private io.delta.kernel.utils.CloseableIterator<Row> currentBatch = null;

    public DeltaInputSourceIterator(io.delta.kernel.utils.CloseableIterator<FilteredColumnarBatch> filteredColumnarBatchCloseableIterator)
    {
      this.filteredColumnarBatchCloseableIterator = filteredColumnarBatchCloseableIterator;
    }

    @Override
    public boolean hasNext()
    {
      if (currentBatch == null)
      {
        while (filteredColumnarBatchCloseableIterator.hasNext())
        {
          currentBatch = filteredColumnarBatchCloseableIterator.next().getRows();
          if (currentBatch.hasNext()) {
            return true;
          }
        }
      }
      return currentBatch != null && currentBatch.hasNext();
    }

    @Override
    public InputRow next()
    {
      return currentBatch.next();
    }

    @Override
    public void close() throws IOException
    {
      filteredColumnarBatchCloseableIterator.close();
    }
  }
}
