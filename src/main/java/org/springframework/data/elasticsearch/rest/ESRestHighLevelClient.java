package org.springframework.data.elasticsearch.rest;

import org.apache.http.Header;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ESRestHighLevelClient extends RestHighLevelClient {

	private boolean forceIndexNameInUrlOfEsRequests;

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
		if (forceIndexNameInUrlOfEsRequests) {
			// Assert only one index in the URL (for AWS AFW security constraints)
			if (request instanceof BulkRequest) {
				BulkRequest bulkRequest = (BulkRequest) request;
				Set<String> indices = bulkRequest.requests().stream().map(DocWriteRequest::index).collect(Collectors.toSet());
				requestConverter = addIndexNameToRequestUrl(indices, requestConverter);
			} else if (request instanceof SearchRequest) {
				SearchRequest searchRequest = (SearchRequest) request;
				Set<String> indices = new HashSet<>(Arrays.asList(searchRequest.indices()));
				assertOneIndex(indices);
			}
		}
		return super.performRequestAndParseEntity(request, requestConverter, entityParser, ignores, headers);
	}

	private <Req extends ActionRequest> CheckedFunction<Req, Request, IOException> addIndexNameToRequestUrl(Set<String> indices, CheckedFunction<Req, Request, IOException> originalRequestConverter) throws IOException {
		assertOneIndex(indices);
		return req -> {
			Request originalRequest = originalRequestConverter.apply(req);
			return new Request(originalRequest.getMethod(), "/" + indices.iterator().next() + originalRequest.getEndpoint(), originalRequest.getParameters(), originalRequest.getEntity());
		};
	}

	private void assertOneIndex(Set<String> indices) throws IOException {
		if (indices.size() != 1) {
			throw new IOException("Bulk requests can only act upon a single index when the forceIndexNameInEsRequestUrl flag is enabled.");
		}
	}

	public void setForceIndexNameInUrlOfEsRequests(boolean forceIndexNameInUrlOfEsRequests) {
		this.forceIndexNameInUrlOfEsRequests = forceIndexNameInUrlOfEsRequests;
	}

	public boolean isForceIndexNameInUrlOfEsRequests() {
		return forceIndexNameInUrlOfEsRequests;
	}
}
