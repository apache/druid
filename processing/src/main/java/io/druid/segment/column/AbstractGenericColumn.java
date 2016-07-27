package io.druid.segment.column;

import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedDoubles;
import io.druid.segment.data.IndexedFloats;
import io.druid.segment.data.IndexedLongs;

import java.io.IOException;

/**
 */
public abstract class AbstractGenericColumn implements GenericColumn
{
  @Override
  public String getStringSingleValueRow(int rowNum)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Indexed<String> getStringMultiValueRow(int rowNum)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloatSingleValueRow(int rowNum)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public IndexedFloats getFloatMultiValueRow(int rowNum)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLongSingleValueRow(int rowNum)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public IndexedLongs getLongMultiValueRow(int rowNum)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDoubleSingleValueRow(int rowNum)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public IndexedDoubles getDoubleMultiValueRow(int rowNum)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException
  {

  }
}
