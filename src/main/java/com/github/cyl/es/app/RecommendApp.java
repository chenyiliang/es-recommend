package com.github.cyl.es.app;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder.Item;
import org.elasticsearch.search.SearchHit;

public class RecommendApp {
	private static final String ES_HOST = "121.40.108.158";
	private static final int ES_PORT = 9301;
	private static final String CLUSTER_NAME = "elasticsearch_tapas_devel";
	private static final String INDEX = "yicai";
	private static final String TYPE = "news";
	private static final String REC_FIELD = "content";
	private static final int RECOMMEND_NUM = 10;
	private static final String[] FETCH_FIELDS = { "title", "origin_url" };
	private static final String IDS_FILE = "d:/data/idList.txt";
	private static final List<String> ID_LIST = new ArrayList<String>();

	static {
		try (BufferedReader br = new BufferedReader(new InputStreamReader(
				new FileInputStream(IDS_FILE), "utf-8"))) {
			String idLine = null;
			while ((idLine = br.readLine()) != null) {
				if (!idLine.trim().isEmpty()) {
					ID_LIST.add(idLine.trim());
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		System.out.println("ID_LIST.size():" + ID_LIST.size());
	}

	public static void main(String[] args) {
		Map<String, List<SearchHit>> recommendMap = new HashMap<String, List<SearchHit>>();

		Client client = createTransportClient(ES_HOST, ES_PORT, CLUSTER_NAME);

		for (int i = 0; i < ID_LIST.size(); i++) {
			String id = ID_LIST.get(i);

			List<SearchHit> recommendHits = new ArrayList<SearchHit>();
			int from = 0;

			while (recommendHits.size() < RECOMMEND_NUM) {
				int fetchNum = RECOMMEND_NUM - recommendHits.size();
				SearchResponse response = client.prepareSearch(INDEX)
						.setTypes(TYPE)
						.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
						.addFields(FETCH_FIELDS).setFrom(from)
						.setSize(fetchNum)
						.setQuery(getRecQuery(INDEX, TYPE, id, REC_FIELD))
						.execute().actionGet();
				SearchHit[] hits = response.getHits().getHits();

				if (hits.length == 0) {
					break;
				}

				for (int j = 0; j < hits.length; j++) {
					SearchHit hit = hits[j];
					if (!checkDuplicated(hit, recommendHits)) {
						recommendHits.add(hit);
					}
				}

				from += fetchNum;
			}

			recommendMap.put(id, recommendHits);
		}

		Set<Entry<String, List<SearchHit>>> entrySet = recommendMap.entrySet();
		for (Entry<String, List<SearchHit>> entry : entrySet) {
			System.out.println(entry.getKey() + ":" + entry.getValue());
		}

		client.close();
	}

	@SuppressWarnings("resource")
	private static Client createTransportClient(String esHost, int esPort,
			String cluserName) {
		final Settings settings = ImmutableSettings.settingsBuilder()
				.put("client.transport.sniff", false)
				.put("cluster.name", cluserName).build();

		return new TransportClient(settings)
				.addTransportAddress(new InetSocketTransportAddress(esHost,
						esPort));
	}

	private static QueryBuilder getMLTQueryForOneIndexedDoc(String index,
			String type, String id, String field) {
		MoreLikeThisQueryBuilder queryBuilder = QueryBuilders
				.moreLikeThisQuery(field).docs(new Item(index, type, id))
				.minWordLength(2).minDocFreq(2).minTermFreq(2)
				.maxQueryTerms(100).analyzer("ik_smart");
		return queryBuilder;
	}

	private static QueryBuilder getRecQuery(String index, String type,
			String id, String field) {
		QueryBuilder mltQuery = getMLTQueryForOneIndexedDoc(index, type, id,
				field);
		BoolFilterBuilder notIdFilter = FilterBuilders.boolFilter().mustNot(
				FilterBuilders.idsFilter(type).addIds(id));
		return QueryBuilders.filteredQuery(mltQuery, notIdFilter);
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

	private static void writeRecResultsToCsv(Map<String, List<SearchHit>> map,
			String outpath) {
		File file = new File(outpath);
		CSVFormat format = CSVFormat.DEFAULT.withRecordSeparator('\n'); // 每条记录间隔符
		if (file.exists()) {
			file.delete();
		}
		Writer writer = null;
		CSVPrinter printer = null;
		try {
			writer = new OutputStreamWriter(new FileOutputStream(file), "utf-8");
			printer = new CSVPrinter(writer, format);
		} catch (IOException e) {
			throw new RuntimeException("Csv File output preparing fails", e);
		}

		Set<Entry<String, List<SearchHit>>> entrySet = map.entrySet();
		for (Entry<String, List<SearchHit>> entry : entrySet) {
			List<String> oneRecord = new ArrayList<String>();
			oneRecord.add(entry.getKey());
			List<SearchHit> hits = entry.getValue();
			for (int i = 0; i < hits.size(); i++) {
				SearchHit hit = hits.get(i);
				oneRecord.add(hit.getId() + "|" + hit.getScore());
			}

			try {
				printer.printRecord(oneRecord);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		try {
			printer.close();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
