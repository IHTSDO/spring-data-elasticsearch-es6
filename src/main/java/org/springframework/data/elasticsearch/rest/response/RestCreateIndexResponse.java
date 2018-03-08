package org.springframework.data.elasticsearch.rest.response;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;

public class RestCreateIndexResponse extends CreateIndexResponse {

	public RestCreateIndexResponse(RestCreateIndexResponsePoJo responsePoJo) {
		super(responsePoJo.isAcknowledged(), responsePoJo.isShardsAcknowledged(), responsePoJo.getIndex());
	}

}
