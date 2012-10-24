package com.metamx.druid.merge;

import com.metamx.druid.client.DataSegment;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

import java.util.List;

/**
 */
@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, property="type", defaultImpl = ClientDefaultMergeQuery.class)
@JsonSubTypes(value={
    @JsonSubTypes.Type(name="append", value=ClientAppendQuery.class)
})
public interface ClientMergeQuery
{
  public String getDataSource();

  public List<DataSegment> getSegments();
}
