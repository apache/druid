package org.apache.druid.indexing.pulsar;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.seekablestream.SeekableStreamDataSourceMetadata;
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamSequenceNumbers;

public class PulsarDataSourceMetadata extends SeekableStreamDataSourceMetadata<Integer, Long>
{
  @JsonCreator
  public PulsarDataSourceMetadata(
      @JsonProperty("partitions") SeekableStreamSequenceNumbers<Integer, Long> pulsarPartitions
  )
  {
    super(pulsarPartitions);
  }

  @Override
  public DataSourceMetadata asStartMetadata()
  {
    final SeekableStreamSequenceNumbers<Integer, Long> sequenceNumbers = getSeekableStreamSequenceNumbers();
    if (sequenceNumbers instanceof SeekableStreamEndSequenceNumbers) {
      return createConcreteDataSourceMetaData(
          ((SeekableStreamEndSequenceNumbers<Integer, Long>) sequenceNumbers).asStartPartitions(true)
      );
    } else {
      return this;
    }
  }

  @Override
  protected SeekableStreamDataSourceMetadata<Integer, Long> createConcreteDataSourceMetaData(
      SeekableStreamSequenceNumbers<Integer, Long> seekableStreamSequenceNumbers
  )
  {
    return new PulsarDataSourceMetadata(seekableStreamSequenceNumbers);
  }
}
