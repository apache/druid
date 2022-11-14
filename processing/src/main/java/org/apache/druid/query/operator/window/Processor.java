package org.apache.druid.query.operator.window;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.query.operator.window.ranking.WindowCumeDistProcessor;
import org.apache.druid.query.operator.window.ranking.WindowDenseRankProcessor;
import org.apache.druid.query.operator.window.ranking.WindowPercentileProcessor;
import org.apache.druid.query.operator.window.ranking.WindowRankProcessor;
import org.apache.druid.query.operator.window.ranking.WindowRowNumberProcessor;
import org.apache.druid.query.operator.window.value.WindowFirstProcessor;
import org.apache.druid.query.operator.window.value.WindowLagProcessor;
import org.apache.druid.query.operator.window.value.WindowLastProcessor;
import org.apache.druid.query.operator.window.value.WindowLeadProcessor;
import org.apache.druid.query.rowsandcols.RowsAndColumns;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "composing", value = ComposingProcessor.class),
    @JsonSubTypes.Type(name = "cumeDist", value = WindowCumeDistProcessor.class),
    @JsonSubTypes.Type(name = "denseRank", value = WindowDenseRankProcessor.class),
    @JsonSubTypes.Type(name = "percentile", value = WindowPercentileProcessor.class),
    @JsonSubTypes.Type(name = "rank", value = WindowRankProcessor.class),
    @JsonSubTypes.Type(name = "rowNumber", value = WindowRowNumberProcessor.class),
    @JsonSubTypes.Type(name = "first", value = WindowFirstProcessor.class),
    @JsonSubTypes.Type(name = "last", value = WindowLastProcessor.class),
    @JsonSubTypes.Type(name = "lead", value = WindowLeadProcessor.class),
    @JsonSubTypes.Type(name = "lag", value = WindowLagProcessor.class),
    @JsonSubTypes.Type(name = "aggregate", value = WindowAggregateProcessor.class),
})
public interface Processor
{
   RowsAndColumns process(RowsAndColumns incomingPartition);
}
