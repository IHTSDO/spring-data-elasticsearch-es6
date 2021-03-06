/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.elasticsearch.core.aggregation.impl;

import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.SearchAfterPage;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Petar Tahchiev
 * @author Artur Konczak
 * @author Mohsin Husen
 */
public class AggregatedPageImpl<T> extends PageImpl<T> implements AggregatedPage<T>, SearchAfterPage<T> {

	private Aggregations aggregations;
	private Map<String, Aggregation> mapOfAggregations = new HashMap<>();
    private String scrollId;
	private Object[] searchAfter;

	public AggregatedPageImpl(List<T> content) {
		super(content);
	}

	public AggregatedPageImpl(List<T> content, String scrollId) {
		super(content);
		this.scrollId = scrollId;
	}

	public AggregatedPageImpl(List<T> content, Pageable pageable, long total, Object[] searchAfter) {
		super(content, pageable, total);
		this.searchAfter = searchAfter;
	}

	public AggregatedPageImpl(List<T> content, Pageable pageable, long total, Aggregations aggregations) {
		super(content, pageable, total);
		this.aggregations = aggregations;
		if (aggregations != null) {
			for (Aggregation aggregation : aggregations) {
				mapOfAggregations.put(aggregation.getName(), aggregation);
			}
		}
	}

	public AggregatedPageImpl(List<T> content, Pageable pageable, long total, Aggregations aggregations, String scrollId, Object[] searchAfter) {
		super(content, pageable, total);
		this.aggregations = aggregations;
		this.scrollId = scrollId;
		this.searchAfter = searchAfter;
		if (aggregations != null) {
			for (Aggregation aggregation : aggregations) {
				mapOfAggregations.put(aggregation.getName(), aggregation);
			}
		}
	}

	@Override
	public boolean hasAggregations() {
		return aggregations != null && mapOfAggregations.size() > 0;
	}

	@Override
	public Aggregations getAggregations() {
		return aggregations;
	}

	@Override
	public Aggregation getAggregation(String name) {
		return aggregations == null ? null : aggregations.get(name);
	}

	@Override
	public String getScrollId() {
		return scrollId;
	}

	@Override
	public Object[] getSearchAfter() {
		return searchAfter;
	}
}
