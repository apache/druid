package com.metamx.druid.index.brita;

import com.metamx.druid.index.v1.processing.Offset;
import com.metamx.druid.kv.Indexed;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;

/**
 */
public interface InvertedIndexSelector
{
  public Indexed<String> getDimensionValues(String dimension);
  public int getNumRows();
  public int[] getInvertedIndex(String dimension, String value);
  public ImmutableConciseSet getConciseInvertedIndex(String dimension, String value);
  public Offset getInvertedIndexOffset(String dimension, String value);
}
