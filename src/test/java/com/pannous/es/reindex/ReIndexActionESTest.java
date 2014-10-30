package com.pannous.es.reindex;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;

public class ReIndexActionESTest extends ReIndexActionTester {

    @Override
    protected MySearchResponse scrollSearch(Client client, String index, String type, String query, int hits,
            boolean withVersion, int keepMinutes) {
        SearchRequestBuilder srb = action.createScrollSearch(client, index, type, query, hits, withVersion, keepMinutes);
        return new MySearchResponseES(client, srb.execute().actionGet(), keepMinutes);
    }
}
