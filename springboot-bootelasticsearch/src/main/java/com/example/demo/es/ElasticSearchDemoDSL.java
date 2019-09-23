package com.example.demo.es;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
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

	// 分页查询
	@Test
	public void testQueryPage() throws IOException {
		SearchRequest searchRequest = new SearchRequest("index_spring");
		searchRequest.types("doc");
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(QueryBuilders.matchAllQuery());
		// 分页查询，下标从0开始
		searchSourceBuilder.from(0);
		searchSourceBuilder.size(2);
		// 设置要显示的字段
		searchSourceBuilder.fetchSource(new String[] { "name", "studymodel", "description" }, new String[] {});
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

	// term query查询
	@Test
	public void testTermQuery() throws IOException {
		SearchRequest searchRequest = new SearchRequest("index_spring");

		searchRequest.types("doc");
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		SearchSourceBuilder query = searchSourceBuilder.query(QueryBuilders.termQuery("name", "22"));
		// source设置要显示的字段
		searchSourceBuilder.fetchSource(new String[] { "name", "studymodel" }, new String[] {});
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

	// 根据id查询
	@Test
	public void testQuerById() throws IOException {
		SearchRequest searchRequest = new SearchRequest("index_spring");
		searchRequest.types("doc");
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		String[] strArray = new String[] { "SFo3WW0B-WexBlSx_UAH", "SVpoWW0B-WexBlSxM0Cr" };
		List<String> asList = Arrays.asList(strArray);
		SearchSourceBuilder query = searchSourceBuilder.query(QueryBuilders.termsQuery("_id", asList));
		// 设置要显示的字段
		searchSourceBuilder.fetchSource(new String[] { "name", "studymodel", "description" }, new String[] {});
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

	// match query的搜索方式
	@Test
	public void testQueryMatch() throws IOException {

		SearchRequest searchRequest = new SearchRequest("index_spring");
		searchRequest.types("doc");
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		// 设置要显示的字段信息
		searchSourceBuilder.fetchSource(new String[] { "name", "studymodel" }, new String[] {});
		searchSourceBuilder.query(QueryBuilders.matchQuery("description", "精通"));
		searchRequest.source(searchSourceBuilder);
		SearchResponse searchResponse = restHighLevelClient.search(searchRequest);

		SearchHits hits = searchResponse.getHits();
		SearchHit[] searchits = hits.getHits();
		for (SearchHit searchHit : searchits) {
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

	// match query查询，指定minimum_should_match的值，
	@Test
	public void testQueryMatchMinimunMatch() throws IOException {

		SearchRequest searchRequest = new SearchRequest("index_spring");
		searchRequest.types("doc");
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		// 设置要显示的字段信息
		searchSourceBuilder.fetchSource(new String[] { "name", "studymodel" }, new String[] {});
		searchSourceBuilder.query(QueryBuilders.matchQuery("description", "精通").minimumShouldMatch("80%"));
		searchRequest.source(searchSourceBuilder);
		SearchResponse searchResponse = restHighLevelClient.search(searchRequest);

		SearchHits hits = searchResponse.getHits();
		SearchHit[] searchits = hits.getHits();
		for (SearchHit searchHit : searchits) {
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

	// multi query，，一次匹配多个字段，matchquery，termquery只能匹配一个字段
	@Test
	public void testQueryMulti() throws IOException {
		SearchRequest searchRequest = new SearchRequest("index_spring");
		searchRequest.types("doc");
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		// 设置要显示的字段信息
		searchSourceBuilder.fetchSource(new String[] { "name", "studymodel", "description" }, new String[] {});
		MultiMatchQueryBuilder multiMatchQuery = QueryBuilders.multiMatchQuery("123", "name", "description")
				.minimumShouldMatch("50%");
		// 设置boost，提升name字段的权重
		multiMatchQuery.field("name", 10f);
		searchSourceBuilder.query(multiMatchQuery);

		searchRequest.source(searchSourceBuilder);
		SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
		SearchHits hits = searchResponse.getHits();
		SearchHit[] searchits = hits.getHits();
		for (SearchHit searchHit : searchits) {
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

	// 布尔查询， must选项，相当于and，should相当于or，must_not相当于not
	@Test
	public void testQueryBoolean() throws IOException {
		SearchRequest searchRequest = new SearchRequest("index_spring");
		searchRequest.types("doc");
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.fetchSource(new String[] { "name", "description" }, new String[] {});
		MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery("123", "name", "description");
		multiMatchQueryBuilder.field("name", 10);
		// term query
		TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("studymodel", "401021");
		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		boolQueryBuilder.must(multiMatchQueryBuilder);
		boolQueryBuilder.must(termQueryBuilder);
		// 设置布尔查询对象
		searchSourceBuilder.query(boolQueryBuilder);

		searchRequest.source(searchSourceBuilder);
		SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
		SearchHits hits = searchResponse.getHits();
		SearchHit[] searchits = hits.getHits();
		for (SearchHit searchHit : searchits) {
			Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
			System.out.println(sourceAsMap);
		}
	}

	// 过滤器
	@Test
	public void testFilter() throws IOException {
		SearchRequest searchRequest = new SearchRequest("index_spring");
		searchRequest.types("doc");
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.fetchSource(new String[] { "name", "description" }, new String[] {});
		searchRequest.source(searchSourceBuilder);
		// 匹配关键字
		MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery("123", "name", "description");
		multiMatchQueryBuilder.field("name", 10);
		searchSourceBuilder.query(multiMatchQueryBuilder);
		// 布尔查询
		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		boolQueryBuilder.must(multiMatchQueryBuilder);
		// 过滤
		boolQueryBuilder.filter(QueryBuilders.termQuery("studymodel", "401021"));
		boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").gte(60).lte(100));

		SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
		SearchHits hits = searchResponse.getHits();
		SearchHit[] searchits = hits.getHits();
		for (SearchHit searchHit : searchits) {
			Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
			System.out.println(sourceAsMap);
		}
	}

	// 排序
	@Test
	public void testQuerySort() throws IOException {
		SearchRequest searchRequest = new SearchRequest("index_spring");
		searchRequest.types("doc");
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.fetchSource(new String[] { "name", "description" }, new String[] {});
		searchRequest.source(searchSourceBuilder);
		// 布尔查询
		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		// 过滤
		boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").gte(60).lte(100));
		// 排序
		searchSourceBuilder.sort(new FieldSortBuilder("studymodel").order(SortOrder.ASC));
		searchSourceBuilder.sort(new FieldSortBuilder("price").order(SortOrder.DESC));

		SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
		SearchHits hits = searchResponse.getHits();
		SearchHit[] searchits = hits.getHits();
		for (SearchHit searchHit : searchits) {
			Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
			System.out.println(sourceAsMap);
		}
	}

	// 高亮显示
	@Test
	public void testHightQuery() throws IOException {
		SearchRequest searchRequest = new SearchRequest("index_spring");
		searchRequest.types("doc");
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.fetchSource(new String[] { "name", "description" }, new String[] {});
		searchRequest.source(searchSourceBuilder);
		// 匹配字段
		MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery("123", "name", "description");
		searchSourceBuilder.query(multiMatchQueryBuilder);
		// 布尔查询
		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		boolQueryBuilder.must(multiMatchQueryBuilder);

		// 过滤，添加条件进行过滤
		boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").gte(60).lte(100));
		// 排序
		searchSourceBuilder.sort(new FieldSortBuilder("studymodel").order(SortOrder.ASC));
		searchSourceBuilder.sort(new FieldSortBuilder("price").order(SortOrder.DESC));
		//高亮显示
		HighlightBuilder highlightBuilder = new HighlightBuilder();
		highlightBuilder.preTags("<tag>");//设置前缀
		highlightBuilder.postTags("</tag>");//设置后缀
		//设置高亮显示字段
		highlightBuilder.fields().add(new HighlightBuilder.Field("name"));
		searchSourceBuilder.highlighter(highlightBuilder);
		
		SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
		SearchHits hits = searchResponse.getHits();
		SearchHit[] searchits = hits.getHits();
		for (SearchHit searchHit : searchits) {
			Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
			String  strname = (String)sourceAsMap.get("name");
			 Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
			if(highlightFields != null) {
				HighlightField highlightName = highlightFields.get("name");
				if(highlightName != null) {
					Text[] fragments = highlightName.getFragments();
					StringBuffer stringBuffer = new StringBuffer();
					for (Text text : fragments) {
						stringBuffer.append(text.string());
					}
					strname = stringBuffer.toString();
				}
			}
			String index = searchHit.getIndex();
			String type = searchHit.getType();
			String id = searchHit.getId();
			float score = searchHit.getScore();
			String sourceAsString = searchHit.getSourceAsString();
			String studymodel = (String) sourceAsMap.get("studymodel");
			String description = (String) sourceAsMap.get("description");
			System.out.println("============分割线================");
			System.out.println(strname);
			System.out.println(studymodel);
			System.out.println(description);
		}
	}
}
