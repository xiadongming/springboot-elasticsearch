package com.example.demo.es;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@SpringBootTest
@RunWith(SpringRunner.class)
public class ElasticSearchDemoDSL {

	@Autowired
	private RestHighLevelClient restHighLevelClient;

	// 查询全部文档
	@Test
	public void testQueryAll() throws IOException {
		SearchRequest searchRequest = new SearchRequest("index_spring");
		searchRequest.types("doc");
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(QueryBuilders.matchAllQuery());
		// source字段过滤
		searchSourceBuilder.fetchSource(new String[] { "name", "studymodel", "description" }, new String[] {});
		searchRequest.source(searchSourceBuilder);
		SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
		SearchHits hits = searchResponse.getHits();
		SearchHit[] searchhits = hits.getHits();

		for (SearchHit searchHit : searchhits) {
			String id = searchHit.getId();
			String type = searchHit.getType();
			String index = searchHit.getIndex();
			float score = searchHit.getScore();
			Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
			String name = (String) sourceAsMap.get("name");
			String studymodel = (String) sourceAsMap.get("studymodel");
			String description = (String) sourceAsMap.get("description");
			System.out.println("============分割线================");
			System.out.println(name);
			System.out.println(studymodel);
			System.out.println(description);

		}
	}

	//分页查询
	@Test
	public void testQueryPage() throws IOException {
		SearchRequest searchRequest = new SearchRequest("index_spring");
		searchRequest.types("doc");
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(QueryBuilders.matchAllQuery());
		//分页查询，下标从0开始
		searchSourceBuilder.from(0);
		searchSourceBuilder.size(2);
		//字段过滤
		searchSourceBuilder.fetchSource(new String[] {"name","studymodel","description"},new String[] {} );
		searchRequest.source(searchSourceBuilder);
		SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
		SearchHits hits = searchResponse.getHits();
		SearchHit[] seqarchits = hits.getHits();
		for (SearchHit searchHit : seqarchits) {
			String id = searchHit.getId();
			String type = searchHit.getType();
			String index = searchHit.getIndex();
			float score = searchHit.getScore();
			Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
			String name = (String) sourceAsMap.get("name");
			String studymodel = (String) sourceAsMap.get("studymodel");
			String description = (String) sourceAsMap.get("description");
			System.out.println("============分割线================");
			System.out.println(name);
			System.out.println(studymodel);
			System.out.println(description);

		}
	
	}
	//term query查询
	@Test
	public void testTermQuery() throws IOException {
		SearchRequest searchRequest = new SearchRequest("index_spring");
		
		searchRequest.types("doc");
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		SearchSourceBuilder query = searchSourceBuilder.query(QueryBuilders.termQuery("name", "22"));
		//source字段过滤
		searchSourceBuilder.fetchSource(new String[] {"name","studymodel"},new String[] {});
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
        SearchHits hits = searchResponse.getHits();
		SearchHit[] seqarchits = hits.getHits();
		for (SearchHit searchHit : seqarchits) {
			String id = searchHit.getId();
			String type = searchHit.getType();
			String index = searchHit.getIndex();
			float score = searchHit.getScore();
			Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
			String name = (String) sourceAsMap.get("name");
			String studymodel = (String) sourceAsMap.get("studymodel");
			String description = (String) sourceAsMap.get("description");
			System.out.println("============分割线================");
			System.out.println(name);
			System.out.println(studymodel);
			System.out.println(description);

		}
	
	 
	}
	
	
	
}
