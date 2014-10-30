package com.pannous.es.reindex;

import org.elasticsearch.action.support.broadcast.BroadcastOperationRequest;

public class ReIndexRequest extends BroadcastOperationRequest<ReIndexRequest> {
	private String index;
	private String searchIndex;
	
	private String type;
	private String searchType;

	
}
