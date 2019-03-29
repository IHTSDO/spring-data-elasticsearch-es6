package org.springframework.data.elasticsearch.core;

import org.springframework.data.domain.*;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.lang.Nullable;

public class SearchAfterPageRequest extends PageRequest {

	private final Object[] searchAfter;

	public SearchAfterPageRequest(Object[] searchAfter, int size) {
		super(0, size);
		this.searchAfter = searchAfter;
	}

	public static SearchAfterPageRequest of(Object[] searchAfter, int size) {
		return new SearchAfterPageRequest(searchAfter, size);
	}

	public Object[] getSearchAfter() {
		return searchAfter;
	}
}
