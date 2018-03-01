package org.springframework.data.elasticsearch.rest.response;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;

public class RestPutMappingResponse extends PutMappingResponse {

	public RestPutMappingResponse(boolean acknowledged) {
		super(acknowledged);
	}
}
