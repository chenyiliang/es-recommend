package com.github.cyl.es.app;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.github.cyl.es.util.NativeClient;

public class ExtractDiffDocIdsFromES {
	private static final String ES_HOST = "121.43.181.142";
	private static final int ES_PORT = 9301;
	private static final String CLUSTER_NAME = "elasticsearch_tapas_devel";
	private static final String INDEX = "hugo-information-development";
	private static final String TYPE = "information";
	private static final String[] FETCH_FIELDS = { "title", "origin_url" };
	private static final String IDS_FILE = "d:/data/idList.txt";

	public static void main(String[] args) {
		Client client = NativeClient.createTransportClient(false, CLUSTER_NAME,
				ES_HOST, ES_PORT);

		SearchRequestBuilder requestBuilder = client.prepareSearch(INDEX)
				.setTypes(TYPE).setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
				.setQuery(QueryBuilders.matchAllQuery())
				.addFields(FETCH_FIELDS);

		int skip = 0;
		int totalNum = 500;
		List<SearchHit> hitList = new ArrayList<SearchHit>();

		while (hitList.size() < totalNum) {
			int fetchNum = totalNum - hitList.size();
			SearchResponse response = requestBuilder.setFrom(skip)
					.setSize(fetchNum).execute().actionGet();
			SearchHit[] hits = response.getHits().hits();
			for (SearchHit hit : hits) {
				if (!checkDuplicated(hit, hitList)) {
					hitList.add(hit);
				}
			}
			skip += fetchNum;
			System.out.println(hitList.size());
		}
		client.close();

		List<String> idList = new ArrayList<String>();
		for (int i = 0; i < hitList.size(); i++) {
			SearchHit hit = hitList.get(i);
			idList.add(hit.getId());
		}

		writeStrListToTxt(idList, IDS_FILE);
	}

	private static boolean checkDuplicated(SearchHit hit,
			List<SearchHit> hitList) {
		try {
			String title = hit.getFields().get("title").getValue();
			String originUrl = hit.getFields().get("origin_url").getValue();
			for (int i = 0; i < hitList.size(); i++) {
				SearchHit hit2 = hitList.get(i);
				String title2 = hit2.getFields().get("title").getValue();
				String originUrl2 = hit2.getFields().get("origin_url")
						.getValue();
				if (title.equals(title2) || originUrl.equals(originUrl2)) {
					return true;
				}
			}
			return false;
		} catch (Exception e) {
			return false;
		}
	}

	private static void writeStrListToTxt(List<String> list, String file) {
		try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
				new FileOutputStream(file), "utf-8"))) {
			for (int i = 0; i < list.size(); i++) {
				String str = list.get(i).trim();
				if (str != null && !str.isEmpty()) {
					bw.write(str);
					bw.newLine();
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
