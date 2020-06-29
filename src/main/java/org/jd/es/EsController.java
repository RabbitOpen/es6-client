package org.jd.es;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.jd.es.entity.Role;
import org.jd.es.entity.User;
import org.jd.es.service.ElasticSearchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * es controller
 * @author xiaoqianbin
 * @date 2020/6/16
 **/
@RestController
@RequestMapping("/es")
public class EsController {

    private AtomicLong index = new AtomicLong(0);

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    ElasticSearchService ess;

    @RequestMapping("/addBatch")
    public void addBatch(@RequestParam("batch") long batch, @RequestParam(name = "batchNo", defaultValue = "1000") long batchNo,
                         @RequestParam(name = "count", defaultValue = "10") int count) throws InterruptedException {
        long begin = System.currentTimeMillis();
        AtomicLong ab = new AtomicLong(batch);
        CountDownLatch cdl = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            new Thread(() -> {
                while (ab.addAndGet(-1) >= 0) {
                    long start = System.currentTimeMillis();
                    List<User> list = new ArrayList<>();
                    for (long j = 0; j < batchNo; j++) {
                        try {
                            list.add(createUser(index.incrementAndGet()));
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                    }
                    ess.addBatch(list, "tuser", "doc");
                    logger.info("{} addBatch cost: {}", Thread.currentThread().getName(), System.currentTimeMillis() - start);
                }
                cdl.countDown();
            }).start();
        }
        cdl.await();
        logger.info("current index: [{}], batch cost: {}, fail: {}", index.get(),
                System.currentTimeMillis() - begin, ess.getFailed());
    }

    @RequestMapping("/queryBatch")
    public String queryBatch(@RequestParam(name = "bizdate", defaultValue = "2020-06-22") String bizdate,
                             @RequestParam(name = "fetchSize", defaultValue = "1500") int fetchSize) {
        long start = System.currentTimeMillis();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        BoolQueryBuilder must = boolQueryBuilder
                .must(QueryBuilders.termQuery("bizdate", bizdate))
                .must(QueryBuilders.rangeQuery("age").lte(10000));
        SearchResponse response = ess.getClient().prepareSearch("tuser").setTypes("doc")
                .setQuery(must)
                .addSort("age", SortOrder.DESC)
                .setFrom(0).setSize(fetchSize)
                .get();
        String info = String.format("cost: %d, hits: %d, size: %d, es cost: %d, ",
                System.currentTimeMillis() - start,
                response.getHits().totalHits,
                response.getHits().getHits().length,
                response.getTook().getMillis()
        );
        logger.info(info);
        return info;
    }

    public static User createUser(long id) throws ParseException {
        User user = new User();
        user.setAddr1("成都市-青羊区-苏坡乡");
        user.setAddr2("成都市-青羊区-苏坡乡");
        user.setAddr3("成都市-青羊区-苏坡乡");
        user.setAddr4("成都市-青羊区-苏坡乡");
        user.setAddr5("成都市-青羊区-苏坡乡");
        user.setAddr6("成都市-青羊区-苏坡乡");
        user.setAddr7("成都市-青羊区-苏坡乡");
        user.setAddr8("成都市-青羊区-苏坡乡");
        user.setAddr9("成都市-青羊区-苏坡乡");
        user.setAddr10("成都市-青羊区-苏坡乡");
        user.setAddr11("成都市-青羊区-苏坡乡");
        user.setAddr12("成都市-青羊区-苏坡乡");
        user.setAddr13("成都市-青羊区-苏坡乡");
        user.setAddr14("成都市-青羊区-苏坡乡");
        user.setAddr15("成都市-青羊区-苏坡乡");
        user.setAddr16("成都市-青羊区-苏坡乡");
        user.setAddr17("成都市-青羊区-苏坡乡");
        user.setAddr18("成都市-青羊区-苏坡乡");
        user.setAddr19("成都市-青羊区-苏坡乡");
        user.setAddr20("成都市-青羊区-苏坡乡");
        user.setAddr21("成都市-青羊区-苏坡乡");
        user.setAddr22("成都市-青羊区-苏坡乡");
        user.setAddr23("成都市-青羊区-苏坡乡");
        user.setAddr24("成都市-青羊区-苏坡乡");
        user.setAddr25("成都市-青羊区-苏坡乡");
        user.setAddr26("成都市-青羊区-苏坡乡");
        user.setAddress("成都市-青羊区-苏坡乡");
        user.setAge(id);
        user.setName("张三-" + id);
        Date d = new Date();
        d.setTime(d.getTime() + id / 1000000 * (24L * 60 * 60 * 1000));
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        user.setBizdate(format.parse(format.format(d)));
        user.setBirthday(new Date());
        Role role = new Role();
        role.setName("manager-" + id / 1000000);
        role.setCode("MANAGER-CODE");
        role.setId(id + "");
        role.setDesc("测试角色");
        user.setRole(role);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        user.setBizdate(sdf.parse(sdf.format(new Date())));
        return user;
    }
}
