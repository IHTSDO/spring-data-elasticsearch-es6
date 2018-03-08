package org.springframework.data.elasticsearch.rest.response;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RestCreateIndexResponsePoJo {

	private boolean acknowledged;

	@JsonProperty("shards_acknowledged")
	private boolean shardsAcknowledged;

	private String index;

	public boolean isAcknowledged() {
		return acknowledged;
	}

	public void setAcknowledged(boolean acknowledged) {
		this.acknowledged = acknowledged;
	}

	public boolean isShardsAcknowledged() {
		return shardsAcknowledged;
	}

	public void setShardsAcknowledged(boolean shardsAcknowledged) {
		this.shardsAcknowledged = shardsAcknowledged;
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}
}
