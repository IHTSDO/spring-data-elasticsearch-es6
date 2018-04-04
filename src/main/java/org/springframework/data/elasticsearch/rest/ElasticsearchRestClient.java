package org.springframework.data.elasticsearch.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.entity.BasicHttpEntity;
import org.elasticsearch.action.*;
import org.elasticsearch.action.admin.indices.close.CloseIndexAction;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsAction;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.elasticsearch.ElasticsearchException;
import org.springframework.data.elasticsearch.rest.request.SingleIndexBulkRequestBuilder;
import org.springframework.data.elasticsearch.rest.response.RestCreateIndexResponse;
import org.springframework.data.elasticsearch.rest.response.RestCreateIndexResponsePoJo;
import org.springframework.data.elasticsearch.rest.response.RestPutMappingResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;

/**
 * ElasticsearchRestClient
 *
 * @author Kai Kewley
 */
public class ElasticsearchRestClient extends AbstractClient {

	private final ESRestHighLevelClient client;
	private final ObjectMapper objectMapper;
	private final Logger logger = LoggerFactory.getLogger(getClass());

	public ElasticsearchRestClient(Map<String, String> settings, RestClientBuilder builder) {
		super(buildSettings(settings), new ThreadPool(buildSettings(settings)));
		client = new ESRestHighLevelClient(builder);
		objectMapper = new ObjectMapper();
	}

	public ElasticsearchRestClient(Map<String, String> settings, String... hosts) {
		this(settings, RestClient.builder(getHttpHosts(hosts)));
	}

	private static HttpHost[] getHttpHosts(String[] hosts) {
		List<HttpHost> httpHosts = new ArrayList<>();
		for (String host : hosts) {
			httpHosts.add(HttpHost.create(host));
		}
		return httpHosts.toArray(new HttpHost[]{});
	}

	@Override
	public BulkRequestBuilder prepareBulk() {
		return new SingleIndexBulkRequestBuilder(this, BulkAction.INSTANCE);
	}

	@Override
	protected <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void doExecute(
			Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> actionListener) {

		try {
			if (action instanceof IndicesExistsAction) {
				// Index Exists
				IndicesExistsRequest indicesExistsRequest = (IndicesExistsRequest) request;
				if (indicesExistsRequest.indices().length != 1) {
					throw new UnsupportedOperationException("Checking the existence of multiple indices at once is not supported.");
				}
				org.elasticsearch.client.Response response = client.getLowLevelClient().performRequest("HEAD", "/" + indicesExistsRequest.indices()[0]);
				boolean exists = response.getStatusLine().getStatusCode() == 200;
				actionListener.onResponse((Response) new IndicesExistsResponse(exists));

			} else if (action instanceof RefreshAction) {
				// Index Refresh
				RefreshRequest refreshRequest = (RefreshRequest) request;
				String[] indices = refreshRequest.indices();
				for (String index : indices) {
					client.getLowLevelClient().performRequest("POST", "/" + index + "/_refresh");
				}
				actionListener.onResponse(null);

			} else if (action instanceof PutMappingAction) {
				// Index Mapping Put
				PutMappingRequest actionRequest = (PutMappingRequest) request;
				if (actionRequest.indices().length != 1) {
					throw new UnsupportedOperationException("Index mapping put request should contain exactly one index name, found " + actionRequest.indices().length);
				}

				String indexName = actionRequest.indices()[0];
				BasicHttpEntity jsonEntity = getJsonHttpEntity(actionRequest.source());
				org.elasticsearch.client.Response response = client.getLowLevelClient().performRequest("PUT", "/" + indexName + "/_mapping/" + actionRequest.type(), Collections.emptyMap(), jsonEntity);
				boolean success = isSuccess(response);
				actionListener.onResponse((Response) new RestPutMappingResponse(success));

			} else if (action instanceof CreateIndexAction) {
				// Create Index API
				CreateIndexRequest actionRequest = (CreateIndexRequest) request;
				BasicHttpEntity jsonEntity = getJsonHttpEntity("{\"settings\":" + objectMapper.writeValueAsString(actionRequest.settings().getAsMap()) + "}}");
				org.elasticsearch.client.Response response = client.getLowLevelClient().performRequest("PUT", "/" + actionRequest.index(), Collections.emptyMap(), jsonEntity);
				boolean success = isSuccess(response);
				RestCreateIndexResponsePoJo responsePoJo = objectMapper.readValue(response.getEntity().getContent(), RestCreateIndexResponsePoJo.class);
				actionListener.onResponse((Response) new RestCreateIndexResponse(responsePoJo));

			} else if (action instanceof DeleteIndexAction) {
				// Delete Index API
				DeleteIndexRequest actionRequest = (DeleteIndexRequest) request;
				org.elasticsearch.client.Response response = client.getLowLevelClient().performRequest("DELETE", "/" + actionRequest.indices()[0]);
				boolean success = isSuccess(response);
				DeleteIndexResponse actionResponse = (DeleteIndexResponse) action.newResponse();
				readSuccessResponse(success, actionResponse);
				actionListener.onResponse((Response) actionResponse);

			} else if (action instanceof OpenIndexAction) {
				// Open Index API
				OpenIndexRequest actionRequest = (OpenIndexRequest) request;
				org.elasticsearch.client.Response response = client.getLowLevelClient().performRequest("POST", "/" + actionRequest.indices()[0] + "/_open");
				boolean success = isSuccess(response);
				OpenIndexResponse actionResponse = (OpenIndexResponse) action.newResponse();
				readSuccessResponse(success, actionResponse);
				actionListener.onResponse((Response) actionResponse);

			} else if (action instanceof CloseIndexAction) {
				// Close Index API
				CloseIndexRequest actionRequest = (CloseIndexRequest) request;
				org.elasticsearch.client.Response response = client.getLowLevelClient().performRequest("POST", "/" + actionRequest.indices()[0] + "/_close");
				boolean success = isSuccess(response);
				CloseIndexResponse actionResponse = (CloseIndexResponse) action.newResponse();
				readSuccessResponse(success, actionResponse);
				actionListener.onResponse((Response) actionResponse);

			} else if (action instanceof IndexAction) {
				// Index API
				IndexRequest actionRequest = (IndexRequest) request;
				IndexResponse actionResponse = client.index(actionRequest);
				actionListener.onResponse((Response) actionResponse);

			} else if (action instanceof GetAction) {
				// Get API
				GetRequest actionRequest = (GetRequest) request;
				GetResponse actionResponse = client.get(actionRequest);
				actionListener.onResponse((Response) actionResponse);

//			} else if (action instanceof MultiGetAction) {
				// Not supported in the Elasticsearch RestHighLevelClient version 6.0.1
				// MultiGet API
//				MultiGetRequest actionRequest = (MultiGetRequest) request;
//				MultiGetResponse actionResponse = client.multiGet(actionRequest);
//				actionListener.onResponse((Response) actionResponse);

			} else if (action instanceof DeleteAction) {
				// Delete API
				DeleteRequest actionRequest = (DeleteRequest) request;
				DeleteResponse actionResponse = client.delete(actionRequest);
				actionListener.onResponse((Response) actionResponse);

			} else if (action instanceof UpdateAction) {
				// Update API
				UpdateRequest actionRequest = (UpdateRequest) request;
				UpdateResponse actionResponse = client.update(actionRequest);
				actionListener.onResponse((Response) actionResponse);

			} else if (action instanceof BulkAction) {
				// Bulk API
				BulkRequest actionRequest = (BulkRequest) request;
				BulkResponse actionResponse = client.bulk(actionRequest);
				actionListener.onResponse((Response) actionResponse);

			} else if (action instanceof SearchAction) {
				// Search API
				SearchRequest actionRequest = (SearchRequest) request;
				SearchResponse actionResponse = client.search(actionRequest);
				actionListener.onResponse((Response) actionResponse);

			} else if (action instanceof SearchScrollAction) {
				// Search Scroll API
				SearchScrollRequest actionRequest = (SearchScrollRequest) request;
				SearchResponse actionResponse = client.searchScroll(actionRequest);
				actionListener.onResponse((Response) actionResponse);

			} else if (action instanceof ClearScrollAction) {
				// Clear Scroll API
				ClearScrollRequest actionRequest = (ClearScrollRequest) request;
				ClearScrollResponse actionResponse = client.clearScroll(actionRequest);
				actionListener.onResponse((Response) actionResponse);

			} else {
				logger.info("Unsupported operation - Action: {}, Request:{}, ActionListener:{}", action, request, actionListener);
				throw new UnsupportedOperationException("Operation is not supported for request type " + action.getClass());
			}
		} catch (IOException e) {
			actionListener.onFailure(e);
		}
	}

	private void readSuccessResponse(boolean success, AcknowledgedResponse actionResponse) throws IOException {
		byte successByte = (byte) (success ? 1 : 0);
		actionResponse.readFrom(new InputStreamStreamInput(new ByteArrayInputStream(new byte[] {successByte})));
	}

	public Map requestGetMappings(String indexName, String type) {
		try {
			Response response = client.getLowLevelClient().performRequest("GET", "/" + indexName + "/_mapping/" + type);
			if (isSuccess(response)) {
				Map map = objectMapper.readValue(response.getEntity().getContent(), Map.class);
				return (Map) ((Map) ((Map) map.getOrDefault(indexName, Collections.emptyMap())).getOrDefault("mappings", Collections.emptyMap())).getOrDefault(type, Collections.emptyMap());
			} else {
				throw new ElasticsearchException("Client failed to get mapping for indexName:" + indexName + ", type:" + type + ", HTTP code:" + response.getStatusLine().getStatusCode() + " " + response.getStatusLine().getReasonPhrase());
			}
		} catch (IOException e) {
			throw new ElasticsearchException("Client failed to get mapping for indexName:" + indexName + ", type:" + type, e);
		}
	}

	public Map requestGetSettings(String indexName) {
		try {
			Response response = client.getLowLevelClient().performRequest("GET", "/" + indexName + "/_settings");
			if (isSuccess(response)) {
				Map map = objectMapper.readValue(response.getEntity().getContent(), Map.class);
				return (Map) ((Map) map.getOrDefault(indexName, Collections.emptyMap())).getOrDefault("settings", Collections.emptyMap());
			} else {
				throw new ElasticsearchException("Client failed to get settings for indexName:" + indexName + ", HTTP code:" + response.getStatusLine().getStatusCode() + " " + response.getStatusLine().getReasonPhrase());
			}
		} catch (IOException e) {
			throw new ElasticsearchException("Client failed to get settings for indexName:" + indexName, e);
		}
	}

	@Override
	public void close() {
		try {
			client.close();
		} catch (IOException e) {
			logger.error("Failed to close Elasticsearch REST client", e);
		}
	}

	private static Settings buildSettings(Map<String, String> settings) {
		if (!settings.containsKey(NODE_NAME_SETTING.getKey())) {
			settings.put(NODE_NAME_SETTING.getKey(), "default");
		}
		Settings.Builder settingsBuilder = Settings.builder();
		for (String key : settings.keySet()) {
			settingsBuilder.put(key, settings.get(key));
		}
		return settingsBuilder.build();
	}

	private BasicHttpEntity getJsonHttpEntity(String source) {
		BasicHttpEntity jsonEntity = new BasicHttpEntity();
		byte[] bytes = source.getBytes();
		jsonEntity.setContent(new ByteArrayInputStream(bytes));
		jsonEntity.setContentLength(bytes.length);
		jsonEntity.setContentType("application/json");
		return jsonEntity;
	}

	private boolean isSuccess(Response response) {
		int statusCode = response.getStatusLine().getStatusCode();
		return (statusCode + "").charAt(0) == '2';
	}
}
