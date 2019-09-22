package com.example.demo.es.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticSearchConfig {

	@Bean
	public RestHighLevelClient restHightLevelClient() {
		
		HttpHost httpHost = new HttpHost("localhost",9200);
		RestHighLevelClient restHighLevelClient = new RestHighLevelClient(RestClient.builder(httpHost));
		
		return restHighLevelClient;
	}
}
