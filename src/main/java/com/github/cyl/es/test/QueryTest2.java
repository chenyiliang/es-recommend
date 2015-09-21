package com.github.cyl.es.test;

import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;

import com.github.cyl.es.query.MoreLikeThisQueryFactory;
import com.github.cyl.es.util.NativeClient;

public class QueryTest2 {

	public static void main(String[] args) {
		String index = "yicai";
		String type = "news";
		String field = "content";
		String id = "559aaf04695a3212ab000000";
		Client client = NativeClient.createTransportClient();
		SearchResponse response = client
				.prepareSearch(index)
				.setTypes(type)
				.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
				.setQuery(
						QueryBuilders
								.matchQuery("origin_url",
										"http://www.cs.com.cn/ssgs/gsxw/201507/t20150706_4750218.html"))
				.setSize(10).addFields("title", "content", "origin_url")
				.execute().actionGet();
		SearchHit[] hits = response.getHits().getHits();
		for (int i = 0; i < hits.length; i++) {
			SearchHit hit = hits[i];
			Map<String, SearchHitField> fields = hit.getFields();
			System.out.println(hit.getId());
			System.out.println("****************************");
		}
	}
}
