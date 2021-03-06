package org.springframework.data.elasticsearch.core.aggregation;

import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.springframework.data.elasticsearch.core.ScrolledPage;

/**
 * @author Petar Tahchiev
 */
public interface AggregatedPage<T> extends ScrolledPage<T> {

	boolean hasAggregations();

	Aggregations getAggregations();

	Aggregation getAggregation(String name);
}
