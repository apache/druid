package org.apache.druid.query.groupby.epinephelinae.column;

import org.apache.druid.segment.ColumnValueSelector;
import org.mockito.Mockito;

import java.util.List;

public class CreateMockColumnValueSelector {
    public static <T> ColumnValueSelector get(Object returnValue) {
        ColumnValueSelector columnValueSelector = Mockito.mock(ColumnValueSelector.class);
        Mockito.when(columnValueSelector.getObject()).thenReturn(returnValue);
        return columnValueSelector;
    }

}
