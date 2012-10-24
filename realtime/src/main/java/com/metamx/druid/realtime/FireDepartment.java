package com.metamx.druid.realtime;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.io.IOException;

/**
 * A Fire Department has a Firehose and a Plumber.
 *
 * This is a metaphor for a realtime stream (Firehose) and a master of sinks (Plumber). The Firehose provides the
 * realtime stream of data.  The Plumber directs each drop of water from the firehose into the correct sink and makes
 * sure that the sinks don't overflow.
 */
public class FireDepartment
{
  @JsonProperty("schema")
  private final Schema schema;

  @JsonProperty
  private final FireDepartmentConfig config;

  @JsonProperty
  private final FirehoseFactory firehoseFactory;

  @JsonProperty
  private final PlumberSchool plumberSchool;

  private final FireDepartmentMetrics metrics = new FireDepartmentMetrics();

  @JsonCreator
  public FireDepartment(
    @JsonProperty("schema") Schema schema,
    @JsonProperty("config") FireDepartmentConfig config,
    @JsonProperty("firehose") FirehoseFactory firehoseFactory,
    @JsonProperty("plumber") PlumberSchool plumberSchool
  )
  {
    this.schema = schema;
    this.config = config;
    this.firehoseFactory = firehoseFactory;
    this.plumberSchool = plumberSchool;
  }

  /**
   * Provides the data schema for the feed that this FireDepartment is in charge of.
   *
   * @return the Schema for this feed.
   */
  public Schema getSchema()
  {
    return schema;
  }

  public FireDepartmentConfig getConfig()
  {
    return config;
  }

  public Plumber findPlumber()
  {
    return plumberSchool.findPlumber(schema, metrics);
  }

  public Firehose connect() throws IOException
  {
    return firehoseFactory.connect();
  }

  public FireDepartmentMetrics getMetrics()
  {
    return metrics;
  }
}
