package io.druid.query.dimension;

import com.google.common.annotations.GwtCompatible;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

import java.io.Serializable;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by yangxu on 5/12/14.
 */
public enum DimensionType {

    INTEGER {
        @Override
        public Comparator getComparator() {
            return Comparators.INTEGER;
        }

        @Override
        public Ordering getOrdering() {
            return NumericOrdering.INSTANCE;
        }

        @Override
        public Comparable cast(String text) {
            try {
                return Long.valueOf(Strings.nullToEmpty(text));
            } catch (NumberFormatException e) {

            }
            return 0l;
        }
    },

    DECIMAL {
        @Override
        public Comparator getComparator() {
            return Comparators.FLOAT;
        }

        @Override
        public Ordering getOrdering() {
            return NumericOrdering.INSTANCE;
        }

        @Override
        public Comparable cast(String text) {
            try {
                return Float.valueOf(Strings.nullToEmpty(text));
            } catch (NumberFormatException e) {

            }
            return 0.0f;
        }
    },
    PLAIN {
        @Override
        public Comparator getComparator() {
            return Comparators.LEXIC;
        }

        @Override
        public Ordering getOrdering() {
            return Ordering.natural();
        }

        @Override
        public Comparable cast(String text) {
            return text;
        }
    };

    public abstract Comparator getComparator();

    public abstract Comparable cast(String text);

    public abstract <C extends Comparable> Ordering<C> getOrdering();
}

final class Comparators {

    public final static Comparator FLOAT =  new Comparator()
    {
        @Override
        public int compare(Object o, Object o1)
        {
            return ((Float) o).compareTo((Float) o1);
        }
    };

    public final static Comparator INTEGER =  new Comparator()
    {
        @Override
        public int compare(Object o, Object o1)
        {
            return ((Long) o).compareTo((Long) o1);
        }
    };


    public final static Comparator LEXIC =  new Comparator()
    {
        @Override
        public int compare(Object o, Object o1)
        {
            return ((String) o).compareTo((String) o1);
        }
    };

}

/** An ordering that uses the natural order of the values. */
@GwtCompatible(serializable = true)
@SuppressWarnings("unchecked") // TODO(kevinb): the right way to explain this??
final class NumericOrdering
        extends Ordering<Comparable> implements Serializable {
    static final NumericOrdering INSTANCE = new NumericOrdering();

    @Override public int compare(Comparable left, Comparable right) {
        checkNotNull(left); // for GWT
        checkNotNull(right);
        if (left == right) {
            return 0;
        }

        return left.compareTo(right);
    }

    @Override public <S extends Comparable> Ordering<S> reverse() {
        return (Ordering<S>) ReverseNumericOrdering.INSTANCE;
    }

    // Override to remove a level of indirection from inner loop
    @Override public int binarySearch(
            List<? extends Comparable> sortedList, Comparable key) {
        return Collections.binarySearch((List) sortedList, key);
    }

    // Override to remove a level of indirection from inner loop
    @Override public <E extends Comparable> List<E> sortedCopy(
            Iterable<E> iterable) {
        List<E> list = Lists.newArrayList(iterable);
        Collections.sort(list);
        return list;
    }

    // preserving singleton-ness gives equals()/hashCode() for free
    private Object readResolve() {
        return INSTANCE;
    }

    @Override public String toString() {
        return "Ordering.numeric()";
    }

    private NumericOrdering() {}

    private static final long serialVersionUID = 1;
}

/** An ordering that uses the reverse of the natural order of the values. */
@GwtCompatible(serializable = true)
@SuppressWarnings("unchecked") // TODO(kevinb): the right way to explain this??
final class ReverseNumericOrdering
        extends Ordering<Comparable> implements Serializable {
    static final ReverseNumericOrdering INSTANCE = new ReverseNumericOrdering();

    @Override public int compare(Comparable left, Comparable right) {
        checkNotNull(left); // right null is caught later
        if (left == right) {
            return 0;
        }

        return right.compareTo(left);
    }

    @Override public <S extends Comparable> Ordering<S> reverse() {
        return Ordering.natural();
    }

    // Override the min/max methods to "hoist" delegation outside loops

    @Override public <E extends Comparable> E min(E a, E b) {
        return NumericOrdering.INSTANCE.max(a, b);
    }

    @Override public <E extends Comparable> E min(E a, E b, E c, E... rest) {
        return NumericOrdering.INSTANCE.max(a, b, c, rest);
    }

    @Override public <E extends Comparable> E min(Iterator<E> iterator) {
        return NumericOrdering.INSTANCE.max(iterator);
    }

    @Override public <E extends Comparable> E min(Iterable<E> iterable) {
        return NumericOrdering.INSTANCE.max(iterable);
    }

    @Override public <E extends Comparable> E max(E a, E b) {
        return NumericOrdering.INSTANCE.min(a, b);
    }

    @Override public <E extends Comparable> E max(E a, E b, E c, E... rest) {
        return NumericOrdering.INSTANCE.min(a, b, c, rest);
    }

    @Override public <E extends Comparable> E max(Iterator<E> iterator) {
        return NumericOrdering.INSTANCE.min(iterator);
    }

    @Override public <E extends Comparable> E max(Iterable<E> iterable) {
        return NumericOrdering.INSTANCE.min(iterable);
    }

    // preserving singleton-ness gives equals()/hashCode() for free
    private Object readResolve() {
        return INSTANCE;
    }

    @Override public String toString() {
        return "Ordering.numeric().reverse()";
    }

    private ReverseNumericOrdering() {}

    private static final long serialVersionUID = 2;
}

