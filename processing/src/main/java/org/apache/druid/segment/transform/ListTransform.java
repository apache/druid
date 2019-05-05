package org.apache.druid.segment.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.Row;
import org.apache.druid.segment.column.ColumnHolder;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ListTransform implements Transform
{
    private final String name;
    private final String fieldName;

    @JsonCreator
    public ListTransform(
            @JsonProperty("name") final String name,
            @JsonProperty("fieldName") final String fieldName
    )
    {
        this.name = Preconditions.checkNotNull(name, "name");
        this.fieldName = Preconditions.checkNotNull(fieldName, "fieldName");
    }

    @JsonProperty
    @Override
    public String getName() {
        return name;
    }

    @JsonProperty
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public RowFunction getRowFunction() {
        return new ListRowFunction(fieldName);
    }

    static class ListRowFunction implements RowFunction {
        private final String fieldName;

        public ListRowFunction(String fieldName) {
           this.fieldName = fieldName;
        }

        public static List<String> split(String str) {
            List<String> list = new ArrayList<>();
            if (str.startsWith("[") && str.endsWith("]")) {
                String containsQuotationStr = str.substring(2, str.length() - 2);

                String[] splitStr = containsQuotationStr.split("\",\"");
                for (String s : splitStr) {
                    list.add(s);
                }
            }

            return list;
        }

        @Override
        public Object eval(Row row) {
            List<String> list = new ArrayList<>();

            Object rawValue = getValueFromRow(row, fieldName);

            if (rawValue != null) {
                // ["welog_bayes_strategy_new","welog_bayes_region_new_strategy"]
                String rawString = rawValue.toString().trim();

                list = split(rawString);
            }

            return list;
        }
    }

    private static Object getValueFromRow(final Row row, final String column)
    {
        if (column.equals(ColumnHolder.TIME_COLUMN_NAME)) {
            return row.getTimestampFromEpoch();
        } else {
            return row.getRaw(column);
        }
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ListTransform that = (ListTransform) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(fieldName, that.fieldName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, fieldName);
    }

    @Override
    public String toString()
    {
        return "ListTransform{" +
                "name='" + name + '\'' +
                ", fieldName='" + fieldName + '\'' +
                '}';
    }

//    public static void main(String[] args) {
//        String testString = "[\"welog_bayes_strategy_new\",\"welog_bayes_region_new_strategy\"]";
//
//        List<String> testRet = ListRowFunction.split(testString);
//
//        for (String s : testRet) {
//            System.out.println(s);
//        }
//
//        String testString2 = "[\"hello\"]";
//
//        List<String> testRet2 = ListRowFunction.split(testString2);
//
//        for (String s : testRet2) {
//            System.out.println(s);
//        }
//
//        String testString3 = "[\"\"]";
//
//        List<String> testRet3 = ListRowFunction.split(testString3);
//
//        for (String s : testRet3) {
//            System.out.println(s);
//        }
//    }
}
