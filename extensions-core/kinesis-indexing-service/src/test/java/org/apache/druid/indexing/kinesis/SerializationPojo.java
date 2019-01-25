package org.apache.druid.indexing.kinesis;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SerializationPojo {
  @JsonProperty
  public String timestamp;

  @JsonProperty
  public String myString;

  @JsonProperty
  public int myInt;

  @JsonProperty
  public double myDouble;
}
