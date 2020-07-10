package org.es.test;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.jd.SpringBootEntry;
import org.jd.es.EsController;
import org.jd.es.entity.User;
import org.jd.es.service.ElasticSearchService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;

/**
 * @author xiaoqianbin
 * @date 2020/6/16
 **/
@RunWith(SpringRunner.class)
@SpringBootTest(classes = SpringBootEntry.class)
public class ElasticSearchTest {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    ElasticSearchService ess;

    @Test
    public void addSingleUser() throws ParseException {
        User user = EsController.createUser(1L);
        ess.addBatch(Arrays.asList(user), "tuser", "doc");
    }

    @Test
    public void query() {
        long start = System.currentTimeMillis();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        BoolQueryBuilder must = boolQueryBuilder
                .must(QueryBuilders.termQuery("bizdate", "2020-06-22"))
                .must(QueryBuilders.rangeQuery("age").lte(10000));
        SearchResponse response = ess.getClient().prepareSearch("tuser").setTypes("doc")
                .setQuery(must)
                .addSort("age", SortOrder.DESC)
                .setFrom(0).setSize(1500)
                .get();
        logger.info("cost: {}, hits: {}, size: {}, es cost: {}", System.currentTimeMillis() - start, response.getHits().totalHits, response.getHits().getHits().length,
                response.getTook().getMillis());
        start = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            boolQueryBuilder = QueryBuilders.boolQuery();
            BoolQueryBuilder m = boolQueryBuilder
                    .must(QueryBuilders.termQuery("bizdate", "2020-06-22"))
                    .must(QueryBuilders.rangeQuery("age").lte(10000));
            response = ess.getClient().prepareSearch("tuser").setTypes("doc")
                    .setQuery(m)
                    .addSort("age", SortOrder.DESC)
                    .setFrom(0).setSize(1500)
                    .get();
        }
        logger.info("cost: {}, hits: {}, size: {}", System.currentTimeMillis() - start, response.getHits().totalHits, response.getHits().getHits().length);
    }

    @Test
    public void reCreateIndex() throws IOException {
        DeleteIndexRequestBuilder delete = ess.getClient().admin().indices().prepareDelete("tuser");
        delete.execute();
        CreateIndexRequestBuilder createIndex = ess.getClient().admin().indices().prepareCreate("tuser");
        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties") //设置之定义字段
                .startObject("name").field("type","keyword").field("index",true).endObject() //设置分析器
                .startObject("age").field("type","long").field("index",true).endObject()
                .startObject("birthday").field("type","date").field("format","date_optional_time||epoch_millis").endObject()
                .startObject("bizdate").field("type","date").field("format","yyyy-MM-dd").field("index",true).endObject()
                .startObject("address").field("type","keyword").field("index",true).endObject()
                .startObject("addr1").field("type","text").endObject()
                .startObject("addr2").field("type","text").endObject()
                .startObject("addr3").field("type","text").endObject()
                .startObject("addr4").field("type","text").endObject()
                .startObject("addr5").field("type","text").endObject()
                .startObject("addr6").field("type","text").endObject()
                .startObject("addr7").field("type","text").endObject()
                .startObject("addr8").field("type","text").endObject()
                .startObject("addr9").field("type","text").endObject()
                .startObject("addr10").field("type","text").endObject()
                .startObject("addr11").field("type","text").endObject()
                .startObject("addr12").field("type","text").endObject()
                .startObject("addr13").field("type","text").endObject()
                .startObject("addr14").field("type","text").endObject()
                .startObject("addr15").field("type","text").endObject()
                .startObject("addr16").field("type","text").endObject()
                .startObject("addr17").field("type","text").endObject()
                .startObject("addr18").field("type","text").endObject()
                .startObject("addr19").field("type","text").endObject()
                .startObject("addr20").field("type","text").endObject()
                .startObject("addr21").field("type","text").endObject()
                .startObject("addr22").field("type","text").endObject()
                .startObject("addr23").field("type","text").endObject()
                .startObject("addr24").field("type","text").endObject()
                .startObject("addr25").field("type","text").endObject()
                .startObject("addr26").field("type","text").endObject()
                .startObject("role").field("type", "nested")
                    .startObject("properties")
                    .startObject("name").field("type", "keyword").field("index", true).endObject()
                    .startObject("code").field("type", "keyword").field("index", true).endObject()
                    .startObject("id").field("type", "keyword").field("index", true).endObject()
                    .startObject("desc").field("type", "text").endObject()
                    .endObject()
                    .endObject()
                .endObject()
                .endObject();
        createIndex.addMapping("doc", mapping);
        CreateIndexResponse res=createIndex.execute().actionGet();
    }


}
