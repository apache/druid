package com.metamx.druid.indexer.updater;

import com.metamx.druid.indexer.DbUpdaterJob;
import com.metamx.druid.indexer.ZkUpdaterJob;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "zk", value = ZkUpdaterJobSpec.class),
    @JsonSubTypes.Type(name = "db", value = DbUpdaterJobSpec.class)
})
public interface UpdaterJobSpec
{
}
