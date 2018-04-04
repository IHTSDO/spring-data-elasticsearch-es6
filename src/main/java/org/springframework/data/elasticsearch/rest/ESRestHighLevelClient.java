package org.springframework.data.elasticsearch.rest;

import org.apache.http.Header;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.springframework.data.elasticsearch.rest.request.SingleIndexBulkRequest;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ESRestHighLevelClient extends RestHighLevelClient {

	public ESRestHighLevelClient(RestClientBuilder restClientBuilder) {
		super(restClientBuilder);
	}

	protected ESRestHighLevelClient(RestClientBuilder restClientBuilder, List<NamedXContentRegistry.Entry> namedXContentEntries) {
		super(restClientBuilder, namedXContentEntries);
	}

	protected ESRestHighLevelClient(RestClient restClient, CheckedConsumer<RestClient, IOException> doClose, List<NamedXContentRegistry.Entry> namedXContentEntries) {
		super(restClient, doClose, namedXContentEntries);
	}

	@Override
	protected <Req extends ActionRequest, Resp> Resp performRequestAndParseEntity(Req request, CheckedFunction<Req, Request, IOException> requestConverter, CheckedFunction<XContentParser, Resp, IOException> entityParser, Set<Integer> ignores, Header... headers) throws IOException {
		// Assert only one index in the URL (for AWS AFW security constraints)
		if (request instanceof SingleIndexBulkRequest) {
			SingleIndexBulkRequest bulkRequest = (SingleIndexBulkRequest) request;
			requestConverter = addIndexNameToRequestUrl(bulkRequest.getIndex(), requestConverter);
		} else if (request instanceof SearchRequest) {
			SearchRequest searchRequest = (SearchRequest) request;
			Set<String> indices = new HashSet<>(Arrays.asList(searchRequest.indices()));
			assertOneIndex(indices);
		}
		return super.performRequestAndParseEntity(request, requestConverter, entityParser, ignores, headers);
	}

	private <Req extends ActionRequest> CheckedFunction<Req, Request, IOException> addIndexNameToRequestUrl(String index, CheckedFunction<Req, Request, IOException> originalRequestConverter) throws IOException {
		return req -> {
			Request originalRequest = originalRequestConverter.apply(req);
			return new Request(originalRequest.getMethod(), "/" + index + originalRequest.getEndpoint(), originalRequest.getParameters(), originalRequest.getEntity());
		};
	}

	private void assertOneIndex(Set<String> indices) throws IOException {
		if (indices.size() != 1) {
			throw new IOException("Bulk requests can only act upon a single index.");
		}
	}

}
