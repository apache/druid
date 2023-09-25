package org.apache.druid.query.scan;

import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.parsers.CloseableIterator;

import java.io.IOException;

/**
 * Iterates over the scan result sequence and provides an interface to clean up the resources (if any) to close the
 * underlying sequence. Similar to {@link Yielder}, once close is called on the iterator, the calls to the rest of the
 * iterator's methods are undefined.
 */
public class ScanResultValueIterator implements CloseableIterator
{
  Yielder<ScanResultValue> yielder;

  public ScanResultValueIterator(final Sequence<ScanResultValue> resultSequence)
  {
    yielder = Yielders.each(resultSequence);
  }

  @Override
  public void close() throws IOException
  {
    yielder.close();
  }

  @Override
  public boolean hasNext()
  {
    return !yielder.isDone();
  }

  @Override
  public Object next()
  {
    ScanResultValue scanResultValue = yielder.get();
    yielder = yielder.next(null);
    return scanResultValue;
  }
}
