package io.druid.segment.incremental;
/**
 * Created by yangxu on 6/9/14.
 */

import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.metamx.common.ISE;
import io.druid.data.input.InputRow;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.ExtractionDimensionSpec;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ExtractionDimensionFormatter {

    private final Map<String, ExtractionDimensionSpec> extractionLookup = new ConcurrentHashMap<>();

    public ExtractionDimensionFormatter(List<DimensionSpec> extractionDimensionSpecs)
    {
        for (DimensionSpec spec : extractionDimensionSpecs) {
            try {
                extractionLookup.put(spec.getOutputName().toLowerCase(), (ExtractionDimensionSpec)spec);
            } catch (ClassCastException e) {
                throw new ISE("Cannot cast DimensionSpec[%s] into ExtractionDimensionSpec", spec);
            }
        }
    }

    public InputRow formatRow(final InputRow row)
    {

        InputRow retVal = new InputRow()
        {
            @Override
            public List<String> getDimensions()
            {
                return row.getDimensions();
            }

            @Override
            public long getTimestampFromEpoch()
            {
                return row.getTimestampFromEpoch();
            }

            @Override
            public List<String> getDimension(String dimension)
            {
                if (Strings.isNullOrEmpty(dimension)) return Collections.EMPTY_LIST;
                final ExtractionDimensionSpec spec = extractionLookup.get(dimension.toLowerCase());
                return null == spec ? row.getDimension(dimension) : Lists.transform(row.getDimension(dimension),
                        new Function<String, String>() {
                            @Override
                            public String apply(String input) {
                                String res =  spec.getDimExtractionFn().apply(input);
                                return res;
                            }
                        }
                );
            }

            @Override
            public Object getRaw(String dimension) {
                return row.getRaw(dimension);
            }

            @Override
            public float getFloatMetric(String metric)
            {
                return row.getFloatMetric(metric);
            }

            @Override
            public String toString()
            {
                return row.toString();
            }
        };

        return retVal;
    }

}
