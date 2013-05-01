package exp.imruVsSpark.kmeans.spark;

import java.util.Iterator;

/**
 * @author Rui Wang
 * @param <T>
 */
public class AggregatedResult<T> implements Iterable {
    T result;

    public AggregatedResult(T result) {
        this.result = result;
    }

    @Override
    public Iterator iterator() {
        return new Iterator<T>() {
            boolean first = true;

            @Override
            public boolean hasNext() {
                return first;
            }

            @Override
            public T next() {
                if (!first)
                    return null;
                first = false;
                return result;
            }

            @Override
            public void remove() {
            }
        };
    }
}
