package com.github.cyl.es.filter;

import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;

public class FilterFactory {
	public static FilterBuilder getNoIdFilter(String id) {
		return FilterBuilders.boolFilter()
				.mustNot(FilterBuilders.idsFilter(id));
	}
}
