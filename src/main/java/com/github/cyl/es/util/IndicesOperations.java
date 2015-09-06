package com.github.cyl.es.util;

import java.util.Map;

import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;

public class IndicesOperations {
	private final Client client;

	public IndicesOperations(Client client) {
		this.client = client;
	}

	// We define a function used to check the index's existence
	public boolean checkIndexExists(String name) {
		IndicesExistsResponse response = client.admin().indices()
				.prepareExists(name).execute().actionGet();
		return response.isExists();
	}

	// We define a function used to create an index
	public void createIndex(String name) {
		CreateIndexResponse response = client.admin().indices()
				.prepareCreate(name).execute().actionGet();
		checkAcknowledgedResponse(response);
	}

	// We define a function used to delete an index
	public void deleteIndex(String name) {
		DeleteIndexResponse response = client.admin().indices()
				.prepareDelete(name).execute().actionGet();
		checkAcknowledgedResponse(response);
	}

	// We define a function used to close an index
	public void closeIndex(String name) {
		CloseIndexResponse response = client.admin().indices()
				.prepareClose(name).execute().actionGet();
		checkAcknowledgedResponse(response);
	}

	// We define a function used to open an index
	public void openIndex(String name) {
		OpenIndexResponse response = client.admin().indices().prepareOpen(name)
				.execute().actionGet();
		checkAcknowledgedResponse(response);
	}

	// Put Mapping
	public void putMapping(String indexName, String typeName,
			Map<String, Object> mappingSource) {
		PutMappingResponse response = client.admin().indices()
				.preparePutMapping(indexName).setType(typeName)
				.setSource(mappingSource).execute().actionGet();
		checkAcknowledgedResponse(response);
	}

	private void checkAcknowledgedResponse(AcknowledgedResponse response) {
		if (!response.isAcknowledged()) {
			throw new RuntimeException(response.getClass().getSimpleName()
					+ " is not acknowledged!");
		}
	}
}
