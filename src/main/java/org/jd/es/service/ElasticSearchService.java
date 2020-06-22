package org.jd.es.service;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import javax.annotation.PostConstruct;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * elastic search 服务类
 * @author xiaoqianbin
 * @date 2020/6/15
 **/
@Service
public class ElasticSearchService {

    private TransportClient client;

    private AtomicLong failure = new AtomicLong();

    @PostConstruct
    public void initialize() throws Exception {
        Settings esSettings = Settings.builder()
                .put("client.transport.sniff", true)
                .build();
        client = new PreBuiltTransportClient(esSettings);
        String[] esHosts = "10.222.16.82".trim().split(",");
        for (String host : esHosts) {
            InetAddress addr = InetAddress.getByName(host);
            client.addTransportAddress(new TransportAddress(addr, 9300));
        }
    }

    /**
     * 添加一个用户
     * @author  xiaoqianbin
     * @date    2020/6/16
     **/
    public void addUser(long id) {
        Map<String, Object> map = createUserMap(id);
        IndexRequestBuilder request = client.prepareIndex("tuser","doc").setSource(map);
        request.execute().actionGet();
    }

    private Map<String, Object> createUserMap(Long id) {
        Map<String, Object> map = new HashMap();
        map.put("name", "张三-" + id);
        map.put("age", id);
        map.put("address", "成都市-青羊区-苏坡乡");
        map.put("addr1", "成都市-青羊区-苏坡乡");
        map.put("addr2", "成都市-青羊区-苏坡乡");
        map.put("addr3", "成都市-青羊区-苏坡乡");
        map.put("addr4", "成都市-青羊区-苏坡乡");
        map.put("addr5", "成都市-青羊区-苏坡乡");
        map.put("addr6", "成都市-青羊区-苏坡乡");
        map.put("addr7", "成都市-青羊区-苏坡乡");
        map.put("addr8", "成都市-青羊区-苏坡乡");
        map.put("addr9", "成都市-青羊区-苏坡乡");
        map.put("addr10", "成都市-青羊区-苏坡乡");
        map.put("addr11", "成都市-青羊区-苏坡乡");
        map.put("addr12", "成都市-青羊区-苏坡乡");
        map.put("addr13", "成都市-青羊区-苏坡乡");
        map.put("addr14", "成都市-青羊区-苏坡乡");
        map.put("addr15", "成都市-青羊区-苏坡乡");
        map.put("addr16", "成都市-青羊区-苏坡乡");
        map.put("addr17", "成都市-青羊区-苏坡乡");
        map.put("addr18", "成都市-青羊区-苏坡乡");
        map.put("addr19", "成都市-青羊区-苏坡乡");
        map.put("addr20", "成都市-青羊区-苏坡乡");
        map.put("addr21", "成都市-青羊区-苏坡乡");
        map.put("addr22", "成都市-青羊区-苏坡乡");
        map.put("addr23", "成都市-青羊区-苏坡乡");
        map.put("addr24", "成都市-青羊区-苏坡乡");
        map.put("addr25", "成都市-青羊区-苏坡乡");
        map.put("addr26", "成都市-青羊区-苏坡乡");
        Date d = new Date();
        d.setTime(d.getTime() + id / 1000000 * (24L * 60 * 60 * 1000));
        map.put("bizDate", new SimpleDateFormat("yyyy-MM-dd").format(d));
        HashMap<Object, Object> role = new HashMap<>();
        role.put("name", "manager-" + id / 1000000);
        role.put("code", "MANAGER-CODE");
        role.put("id", id);
        map.put("role", role);
        map.put("birthday", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new Date()));
        return map;
    }

    /**
     * 批量添加数据
     * @param	ids
     * @author  xiaoqianbin
     * @date    2020/6/16
     **/
    public void bulkAdd(List<Long> ids) {
        BulkRequestBuilder bulk = client.prepareBulk();
        for (Long id : ids) {
            bulk.add(client.prepareIndex("tuser","doc").setSource(createUserMap(id)));
        }
        bulk.execute(new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse responses) {
                logger.info("bulk add succeed！ cost: {}", responses.getTook().getMillis());
            }
            @Override
            public void onFailure(Exception e) {
                failure.getAndAdd(1L);
            }
        });
        clearRequestData(bulk);


    }

    protected void clearRequestData(BulkRequestBuilder bulk) {
        try {
            Field reqField = ActionRequestBuilder.class.getDeclaredField("request");
            reqField.setAccessible(true);
            BulkRequest req = (BulkRequest) reqField.get(bulk);
            Field indicesField = req.getClass().getDeclaredField("requests");
            indicesField.setAccessible(true);
            indicesField.set(req, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Logger logger = LoggerFactory.getLogger(getClass());

    public TransportClient getClient() {
        return client;
    }

    public AtomicLong getFailure() {
        return failure;
    }

}
