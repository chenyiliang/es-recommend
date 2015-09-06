package com.github.cyl.es.query;

import org.elasticsearch.index.query.MoreLikeThisQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder.Item;

public class MoreLikeThisQueryFactory {
	/**
	 * 
	 * @param index
	 * @param type
	 * @param id
	 * @param field
	 * @desc min_word_length:2; min_doc_freq:2; min_term_freq:2;
	 *       max_query_terms:100; analyzer:ik_smart;
	 */
	public static QueryBuilder getMLTQueryForOneIndexedDoc(String index,
			String type, String id, String field) {
		MoreLikeThisQueryBuilder queryBuilder = QueryBuilders
				.moreLikeThisQuery(field).docs(new Item(index, type, id))
				.minWordLength(2).minDocFreq(2).minTermFreq(2)
				.maxQueryTerms(100).analyzer("ik_smart");
		return queryBuilder;
	}
}
