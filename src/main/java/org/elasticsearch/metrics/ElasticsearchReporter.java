/*
 * Licensed to Elasticsearch under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.metrics;

import static com.codahale.metrics.MetricRegistry.name;
import static org.elasticsearch.metrics.JsonMetrics.*;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;

public class ElasticsearchReporter extends ScheduledReporter {

	public static Builder forRegistry(MetricRegistry registry) {
		return new Builder(registry);
	}

	public static class Builder {
		private final MetricRegistry registry;
		private Clock clock;
		private String prefix;
		private TimeUnit rateUnit;
		private TimeUnit durationUnit;
		private MetricFilter filter;
		private String[] hosts = new String[] { "localhost:9200" };
		private String index = "metrics";
		private String indexDateFormat = "yyyy-MM";
		private String docType = "doc";
		private int bulkSize = 2500;
		private int timeout = 1000;
		private String timestampFieldname = "@timestamp";
		private Map<String, Object> additionalFields;
		private MetricElasticsearchClient elasticsearchClient;

		private Builder(MetricRegistry registry) {
			this.registry = registry;
			this.clock = Clock.defaultClock();
			this.prefix = null;
			this.rateUnit = TimeUnit.SECONDS;
			this.durationUnit = TimeUnit.MILLISECONDS;
			this.filter = MetricFilter.ALL;
		}

		/**
		 * Inject your custom definition of how time passes. Usually the default
		 * clock is sufficient
		 */
		public Builder withClock(Clock clock) {
			this.clock = clock;
			return this;
		}

		/**
		 * Configure a prefix for each metric name. Optional, but useful to
		 * identify single hosts
		 */
		public Builder prefixedWith(String prefix) {
			this.prefix = prefix;
			return this;
		}

		/**
		 * Convert all the rates to a certain timeunit, defaults to seconds
		 */
		public Builder convertRatesTo(TimeUnit rateUnit) {
			this.rateUnit = rateUnit;
			return this;
		}

		/**
		 * Convert all the durations to a certain timeunit, defaults to
		 * milliseconds
		 */
		public Builder convertDurationsTo(TimeUnit durationUnit) {
			this.durationUnit = durationUnit;
			return this;
		}

		/**
		 * Allows to configure a special MetricFilter, which defines what
		 * metrics are reported
		 */
		public Builder filter(MetricFilter filter) {
			this.filter = filter;
			return this;
		}

		/**
		 * The index name to index in
		 */
		public Builder index(String index) {
			this.index = index;
			return this;
		}

		/**
		 * The index date format used for rolling indices This is appended to
		 * the index name, split by a '-'
		 */
		public Builder indexDateFormat(String indexDateFormat) {
			this.indexDateFormat = indexDateFormat;
			return this;
		}

		/**
		 * The bulk size per request, defaults to 2500 (as metrics are quite
		 * small)
		 */
		public Builder bulkSize(int bulkSize) {
			this.bulkSize = bulkSize;
			return this;
		}

		/**
		 * Configure the name of the timestamp field, defaults to '@timestamp'
		 */
		public Builder timestampFieldname(String fieldName) {
			this.timestampFieldname = fieldName;
			return this;
		}

		/**
		 * Additional fields to be included for each metric
		 * 
		 * @param additionalFields
		 * @return
		 */
		public Builder additionalFields(Map<String, Object> additionalFields) {
			this.additionalFields = additionalFields;
			return this;
		}

		public Builder setDocType(String docType) {
			this.docType = docType;
			return this;
		}

		public Builder setElasticsearchClient(MetricElasticsearchClient elasticsearchClient) {
			this.elasticsearchClient = elasticsearchClient;
			return this;
		}

		public ElasticsearchReporter build() {
			return new ElasticsearchReporter(registry, elasticsearchClient, index, docType, indexDateFormat, bulkSize, clock, prefix, rateUnit, durationUnit, filter, timestampFieldname,
					additionalFields);
		}
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchReporter.class);

	private final Clock clock;
	private final String prefix;
	private final String index;
	private final String docType;
	private final int bulkSize;
	private String timestampFieldname;

	private final ObjectMapper objectMapper = new ObjectMapper();
	private final ObjectWriter writer;
	private MetricFilter percolationFilter;
	private DateTimeFormatter indexDateFormat = null;
	private boolean checkedForIndexTemplate = false;
	private MetricElasticsearchClient elasticsearchClient;

	public ElasticsearchReporter(MetricRegistry registry, MetricElasticsearchClient elasticsearchClient, String index, String docType, String indexDateFormat, int bulkSize, Clock clock, String prefix,
			TimeUnit rateUnit, TimeUnit durationUnit, MetricFilter filter, String timestampFieldname, Map<String, Object> additionalFields) {
		super(registry, "elasticsearch-reporter", filter, rateUnit, durationUnit);
		this.index = index;
		this.bulkSize = bulkSize;
		this.clock = clock;
		this.prefix = prefix;
		this.elasticsearchClient = elasticsearchClient;
		this.docType = docType;
		if (indexDateFormat != null && indexDateFormat.length() > 0) {
			this.indexDateFormat = DateTimeFormatter.ofPattern(indexDateFormat);
		}

		if (timestampFieldname == null || timestampFieldname.trim().length() == 0) {
			LOGGER.error("Timestampfieldname {} is not valid, using default @timestamp", timestampFieldname);
			this.timestampFieldname = "@timestamp";
		} else {
			this.timestampFieldname = timestampFieldname;
		}

		objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
		objectMapper.configure(SerializationFeature.CLOSE_CLOSEABLE, false);
		// auto closing means, that the objectmapper is closing after the first
		// write call, which does not work for bulk requests
		objectMapper.configure(JsonGenerator.Feature.AUTO_CLOSE_JSON_CONTENT, false);
		objectMapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
		objectMapper.registerModule(new AfterburnerModule());
		// objectMapper.registerModule(new
		// MetricsModule(rateUnit,durationUnit,false,filter));
		objectMapper.registerModule(new MetricsElasticsearchModule(rateUnit, durationUnit, timestampFieldname, additionalFields));
		writer = objectMapper.writer();
		// checkForIndexTemplate();
	}

	private String buildIndexName(String type, String indexDateSuffix) {
		String currentIndexName = index + "-" + type;
		if (indexDateFormat != null) {
			currentIndexName += indexDateSuffix;
		}
		return currentIndexName;
	}

	private Optional<byte[]> convert(JsonMetric jsonMetric) {
		byte[] data;
		try {
			data = objectMapper.writeValueAsBytes(jsonMetric);
		} catch (JsonProcessingException e) {
			data = null;
			e.printStackTrace();
		}
		return Optional.of(data);
	}

	@Override
	public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters,
			SortedMap<String, Timer> timers) {

		// nothing to do if we dont have any metrics to report
		if (gauges.isEmpty() && counters.isEmpty() && histograms.isEmpty() && meters.isEmpty() && timers.isEmpty()) {
			LOGGER.info("All metrics empty, nothing to report");
			return;
		}

		if (!checkedForIndexTemplate) {
			elasticsearchClient.checkForIndexTemplate(this.index, this.docType, this.timestampFieldname);
		}
		final long timestamp = clock.getTime() / 1000;

		String indexDateSuffix = "-" + LocalDateTime.ofEpochSecond(timestamp, 0, ZoneOffset.UTC).format(indexDateFormat);

		List<byte[]> jsonMetrics = new ArrayList<>();
		for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
			if (entry.getValue().getValue() != null) {
				JsonMetric jsonMetric = new JsonGauge(name(prefix, entry.getKey()), timestamp, entry.getValue());
				convert(jsonMetric).ifPresent(jsonMetrics::add);
			}
		}

		write(indexDateSuffix, jsonMetrics, JsonGauge.TYPE);

		for (Map.Entry<String, Counter> entry : counters.entrySet()) {
			JsonCounter jsonMetric = new JsonCounter(name(prefix, entry.getKey()), timestamp, entry.getValue());
			convert(jsonMetric).ifPresent(jsonMetrics::add);
		}

		write(indexDateSuffix, jsonMetrics, JsonCounter.TYPE);

		for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
			JsonHistogram jsonMetric = new JsonHistogram(name(prefix, entry.getKey()), timestamp, entry.getValue());
			convert(jsonMetric).ifPresent(jsonMetrics::add);
		}
		write(indexDateSuffix, jsonMetrics, JsonHistogram.TYPE);

		for (Map.Entry<String, Meter> entry : meters.entrySet()) {
			JsonMeter jsonMetric = new JsonMeter(name(prefix, entry.getKey()), timestamp, entry.getValue());
			convert(jsonMetric).ifPresent(jsonMetrics::add);
		}
		write(indexDateSuffix, jsonMetrics, JsonMeter.TYPE);
		for (Map.Entry<String, Timer> entry : timers.entrySet()) {
			JsonTimer jsonMetric = new JsonTimer(name(prefix, entry.getKey()), timestamp, entry.getValue());
			convert(jsonMetric).ifPresent(jsonMetrics::add);
		}
		write(indexDateSuffix, jsonMetrics, JsonTimer.TYPE);

	}

	private void write(String indexDateSuffix, List<byte[]> data, String type) {
		if (!data.isEmpty()) {
			String currentIndexName = buildIndexName(type, indexDateSuffix);
			elasticsearchClient.writeJsonMetricAsync(currentIndexName, this.docType, data, this.bulkSize);
			data.clear();
		}
	}

}
