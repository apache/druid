package org.apache.druid.indexing.kinesis.common;

public class SequenceNumberPlus
{
  private final String sequenceNumber;
  private final boolean exclusive;

  private SequenceNumberPlus(String sequenceNumber, boolean exclusive)
  {
    this.sequenceNumber = sequenceNumber;
    this.exclusive = exclusive;
  }

  public String get()
  {
    return sequenceNumber;
  }

  public boolean isExclusive()
  {
    return exclusive;
  }

  public static SequenceNumberPlus of(String sequenceNumber, boolean exclusive)
  {
    return new SequenceNumberPlus(sequenceNumber, exclusive);
  }
}
