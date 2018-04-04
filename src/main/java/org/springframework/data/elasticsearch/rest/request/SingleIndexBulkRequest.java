package org.springframework.data.elasticsearch.rest.request;

import org.elasticsearch.action.bulk.BulkRequest;

public class SingleIndexBulkRequest extends BulkRequest {

	private final String index;

	public SingleIndexBulkRequest(String index) {
		this.index = index;
	}

	public String getIndex() {
		return index;
	}
}
