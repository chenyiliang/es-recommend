package com.github.cyl.es.util;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class NativeClient {
	// https://discuss.elastic.co/t/recovering-from-nonodeavailableexception/25093/2
	// 根据实验貌似，当client.transport.sniff=true时，如果该节点无法访问网络，则会抛出None of the
	// configured nodes are available
	@SuppressWarnings("resource")
	public static Client createTransportClient() {
		final Settings settings = ImmutableSettings.settingsBuilder()
				.put("client.transport.sniff", false)
				.put("cluster.name", "elasticsearch_tapas_devel").build();

		return new TransportClient(settings)
				.addTransportAddress(new InetSocketTransportAddress(
						"121.40.108.158", 9301));
	}

	@SuppressWarnings("resource")
	public static Client createTransportClient(boolean sniff,
			String clusterName, String esHost, int esPort) {
		final Settings settings = ImmutableSettings.settingsBuilder()
				.put("client.transport.sniff", sniff)
				.put("cluster.name", clusterName).build();

		return new TransportClient(settings)
				.addTransportAddress(new InetSocketTransportAddress(esHost,
						esPort));
	}

	public static void main(String[] args) {
		Client client = createTransportClient();
		IndicesExistsResponse response = client.admin().indices()
				.prepareExists("xxxx").execute().actionGet();
		System.out.println(response.isExists());
	}
}
