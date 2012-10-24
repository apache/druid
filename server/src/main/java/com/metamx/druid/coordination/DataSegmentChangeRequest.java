package com.metamx.druid.coordination;

import com.metamx.druid.client.DataSegment;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

/**
 */
@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, property="action")
@JsonSubTypes(value={
    @JsonSubTypes.Type(name="load", value=SegmentChangeRequestLoad.class),
    @JsonSubTypes.Type(name="drop", value=SegmentChangeRequestDrop.class)
})
public interface DataSegmentChangeRequest
{
  public void go(DataSegmentChangeHandler handler);
  public DataSegment getSegment();
}
