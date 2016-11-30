package io.druid.timeline;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Interval;

import java.util.Map;

/**
 * Created by haoxiang on 16/11/11.
 */
public class TaskDataSegment
{

    private final String type;
    private final String id;
    private final String groupId;
    private final String dataSource;
    private final Map<String, Object> ioConfig;
    private final Interval interval;

    @JsonCreator
    public TaskDataSegment(
            @JsonProperty("type") String type,
            @JsonProperty("id") String id,
            @JsonProperty("groupId") String groupId,
            @JsonProperty("dataSource") String dataSource,
            @JsonProperty("ioConfig") Map<String, Object> ioConfig,
            @JsonProperty("interval") Interval interval
    )
    {
        this.type = type;
        this.id = id;
        this.groupId = groupId;
        this.dataSource = dataSource;
        this.ioConfig = ioConfig;
        this.interval = interval;
    }

    /**
     * Get dataSource
     *
     * @return the dataSource
     */

    @JsonProperty
    public String getType()
    {
        return type;
    }

    @JsonProperty
    public String getId()
    {
        return id;
    }

    @JsonProperty
    public String getGroupId()
    {
        return groupId;
    }

    @JsonProperty
    public String getDataSource()
    {
        return dataSource;
    }

    @JsonProperty
    public Map<String, Object> getIoConfig()
    {
        return ioConfig;
    }

    @JsonProperty
    public Interval getInterval()
    {
        return interval;
    }

    @Override
    public String toString()
    {
        return "TaskDataSegment{" +
                "type=" + type +
                ", id=" + id +
                ", groupId=" + groupId +
                ", dataSource=" + dataSource +
                '}';
    }


}