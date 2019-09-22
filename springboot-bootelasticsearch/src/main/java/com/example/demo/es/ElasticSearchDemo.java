package com.example.demo.es;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
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
public class ElasticSearchDemo {

	@Autowired
	private RestHighLevelClient restHighLevelClient;

	// 创建索引库
	@Test
	public void testCreatIndex() throws IOException {
		CreateIndexRequest createIndexRequest = new CreateIndexRequest("index_spring");
		createIndexRequest.settings(Settings.builder().put("number_of_shards", 1));
		// 设置映射 mapping，，，此处省略
		String mappingstring = "{\r\n" + "\"properties\": {\r\n" + "\"name\": {\r\n" + "\"type\": \"text\"\r\n"
				+ "},\r\n" + "\"description\": {\r\n" + "\"type\": \"text\"\r\n" + "},\r\n" + "\"studymodel\": {\r\n"
				+ "\"type\": \"keyword\"\r\n" + "},\r\n" + "\"price\": {\r\n" + "\"type\": \"float\"\r\n" + "},\r\n"
				+ "\"timestamp\": {\r\n" + "\"type\": \"date\",\r\n"
				+ "\"format\": \"yyyy-MM-dd HH:mm:ss||yyyy‐MM‐dd||epoch_millis\"\r\n" + "}\r\n" + "}\r\n" + "}";
		createIndexRequest.mapping("doc", mappingstring, XContentType.JSON);
		// 创建索引，操作客户端
		IndicesClient indices = restHighLevelClient.indices();
		// 创建响应对象
		CreateIndexResponse createIndexResponse = indices.create(createIndexRequest);
		boolean acknowledged = createIndexResponse.isAcknowledged();
		System.out.println("============分割线================");
		System.out.println(acknowledged);
	}

	// 删除索引库

	@Test
	public void testDeleteIndex() throws IOException {
		DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("index_spring");

		AcknowledgedResponse acknowledgedResponse = restHighLevelClient.indices().delete(deleteIndexRequest);
		boolean acknowledged = acknowledgedResponse.isAcknowledged();
		System.out.println("=====================分割线================");
		System.out.println(acknowledged);
	}

	// 添加文档
	@Test
	public void testAddDocment() throws IOException {
		Map<String,Object> jsonMap = new HashMap();
       jsonMap.put("name","添加文档123");
       jsonMap.put("description", "精通--大数据的elasticsearch精通===精通");
       jsonMap.put("studymodel", "401021");
       jsonMap.put("price", 5.8f);
       IndexRequest indexRequest = new IndexRequest("index_spring","doc");
       indexRequest.source(jsonMap);
       IndexResponse indexResponse = restHighLevelClient.index(indexRequest);
       
       Result result = indexResponse.getResult();
       System.out.println("======================分割线====================");
       System.out.println(result);
	}
   
	//查询一条文档
	@Test
	public void testQueryDocment() throws IOException {
		GetRequest getRequest = new GetRequest("index_spring","doc","R1o0WW0B-WexBlSxpECC");//"R1o0WW0B-WexBlSxpECC"
		GetResponse getResponse = restHighLevelClient.get(getRequest);
		boolean exists = getResponse.isExists();
		Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
		System.out.println("=========分割线==============");
		System.out.println(sourceAsMap);
		
	}
	
	
	
	
	
	//更新文档
	@Test
	public void testUpdateDocment() throws IOException {
		UpdateRequest updateRequest = new UpdateRequest("index_spring","doc","R1o0WW0B-WexBlSxpECC");
		Map<String,Object> hashMap = new HashMap();
		hashMap.put("name", "xiadongming");
		updateRequest.doc(hashMap);
		
		UpdateResponse update = restHighLevelClient.update(updateRequest);
		RestStatus status = update.status();
		System.out.println("=========分割线==============");
		System.out.println(status);
	}
	
	//删除文档
	@Test
	public void testDeleteDocment() throws IOException {
		DeleteRequest deleteIndexRequest = new DeleteRequest("index_spring","doc","R1o0WW0B-WexBlSxpECC");
	
		DeleteResponse deleteResponse = restHighLevelClient.delete(deleteIndexRequest);
		
		Result result = deleteResponse.getResult();
		System.out.println("==================分割线=================");
	   System.out.println(result);
	}
	
	
	
	
	
	
}
