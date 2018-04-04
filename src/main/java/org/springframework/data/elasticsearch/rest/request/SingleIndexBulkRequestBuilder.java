package org.springframework.data.elasticsearch.rest.request;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.springframework.data.elasticsearch.ElasticsearchException;

public class SingleIndexBulkRequestBuilder extends BulkRequestBuilder {

	private String index = "";

	public SingleIndexBulkRequestBuilder(ElasticsearchClient client, BulkAction action) {
		super(client, action);
	}

	@Override
	public BulkRequestBuilder add(IndexRequestBuilder request) {
		setIndex(request.request().index());
		// Clear index from the individual request, it will be included in the bulk request URL.
		request.setIndex("");
		return super.add(request);
	}

	@Override
	public BulkRequestBuilder add(UpdateRequestBuilder request) {
		setIndex(request.request().index());
		request.setIndex("");
		return super.add(request);
	}

	@Override
	public BulkRequestBuilder add(DeleteRequestBuilder request) {
		setIndex(request.request().index());
		request.setIndex("");
		return super.add(request);
	}

	private void setIndex(String requestIndex) {
		if (index != null && !index.isEmpty() && !index.equals(requestIndex)) {
			throw new ElasticsearchException("All requests within a bulk request must use the same index. " + index + " and " + requestIndex + " given.");
		} else {
			index = requestIndex;
		}
	}

	@Override
	public ActionFuture<BulkResponse> execute() {
		SingleIndexBulkRequest singleIndexBulkRequest = new SingleIndexBulkRequest(index);
		for (DocWriteRequest docWriteRequest : request.requests()) {
			singleIndexBulkRequest.add(docWriteRequest);
		}
		return client.execute(action, singleIndexBulkRequest);
	}
}
