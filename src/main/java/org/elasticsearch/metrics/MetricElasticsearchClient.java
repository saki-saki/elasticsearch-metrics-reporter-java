package org.elasticsearch.metrics;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

/**
 * @author ikas
 */
public class MetricElasticsearchClient {

	private TransportClient client;

	private String clusterName;

	private List<InetSocketAddress> addresses;

	private boolean sniff;

	public MetricElasticsearchClient(String clusterName, List<InetSocketAddress> addresses, boolean sniff) {
		this.clusterName = clusterName;
		this.addresses = addresses;
		this.sniff = sniff;
		build();
	}

	public MetricElasticsearchClient(Object client) {
		this.client = (TransportClient) client;
	}

	protected void build() {
		Settings settings = Settings.builder().put("client.transport.sniff", this.sniff).put("cluster.name", this.clusterName).build();
		this.client = new PreBuiltTransportClient(settings);
		this.addresses.stream().map(x -> new TransportAddress(x)).forEach(client::addTransportAddress);
	}

	protected void writeJsonMetricAsync(String indexName, String docType, List<byte[]> data, int bulkSize) {
		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
		int i = 0;
		for (byte[] bytes : data) {
			try {
				IndexRequestBuilder request = client.prepareIndex(indexName, docType).setSource(bytes, XContentType.JSON);
				bulkRequestBuilder.add(request);
				i++;
			} catch (Exception e) {
				e.printStackTrace();
			}
			if (i >= bulkSize) {
				bulkRequestBuilder.execute();
				i = 0;
			}
		}
		if (i > 0) {
			bulkRequestBuilder.execute();
		}
	}

	/**
	 * This index template is automatically applied to all indices which start
	 * with the index name The index template simply configures the name not to
	 * be analyzed
	 */
	protected void checkForIndexTemplate(String index,String docType,String timeFieldName) {
		String name = index + "-template";
		GetIndexTemplatesResponse response = client.admin().indices().prepareGetTemplates(name).execute().actionGet();
		if (response.getIndexTemplates().size() > 0) {
			return;
		}

		try {
			XContentBuilder json = JsonXContent.contentBuilder();
			json.startObject();
			json.field("template", index + "*");


			json.startObject("mappings");
				json.startObject(docType);
					json.startObject("properties");
							json.startObject(timeFieldName);
							json.field("type", "date");
							json.field("format", "epoch_millis");
							json.endObject();
					json.endObject();
				json.endObject();
			json.endObject();

//			"dynamic_templates": [
//			{
//				"strings_as_keyword": {
//				"mapping": {
//					"type": "keyword",
//							"ignore_above": 1024
//				},
//				"match_mapping_type": "string"
//			}
//			}
//    ]
//
//		}

			json.startArray("dynamic_templates");
				json.startObject();
					json.startObject("strings_as_keyword");
						json.startObject("mapping");
						json.field("type", "keyword");
						json.field("ignore_above", "256");
						json.endObject();
					json.field("match_mapping_type", "string");
					json.endObject();
				json.endObject();
			json.endArray();


			json.endObject();
			client.admin().indices().preparePutTemplate(name).setPatterns(Arrays.asList(index + "-*")).setSource(json).execute().actionGet();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
