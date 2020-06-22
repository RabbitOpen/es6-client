package org.jd.es.service;

import com.fasterxml.jackson.annotation.JsonFormat;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.jd.es.entity.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * es6 client service
 * @author xiaoqianbin
 * @date 2020/6/22
 **/
@Service
public class ElasticSearchService {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private TransportClient client;

    @Value("${es.hosts:10.222.16.82}")
    private String esHosts;

    @Value("${es.port:9300}")
    private int esPort;

    // 失败次数
    private AtomicLong failed = new AtomicLong(0);

    @PostConstruct
    public void init() throws UnknownHostException {
        Settings esSettings = Settings.builder()
                .put("client.transport.sniff", true).build();
        client = new PreBuiltTransportClient(esSettings);
        String[] hosts = esHosts.trim().split(",");
        for (String host : hosts) {
            InetAddress addr = InetAddress.getByName(host);
            client.addTransportAddress(new TransportAddress(addr, esPort));
        }
    }

    /**
     * 批量添加数据
     * @param	list
     * @param	index   索引
     * @param	type    表
     * @author  xiaoqianbin
     * @date    2020/6/22
     **/
    public void addBatch(List<User> list, String index, String type) {
        BulkRequestBuilder bulk = client.prepareBulk();
        for (User o : list) {
            bulk.add(client.prepareIndex(index, type)
                    //insert ingore
//                    .setOpType(DocWriteRequest.OpType.CREATE)
                    .setId(o.getAge() + "").setSource(toMap(o)));
        }
        BulkResponse bulkItemResponses = bulk.execute().actionGet();
        bulkItemResponses.hasFailures();
    }

    /**
     * 获取失败次数
     * @param
     * @author  xiaoqianbin
     * @date    2020/6/22
     **/
    public long getFailed() {
        return failed.get();
    }

    /**
     * object to map
     * @param	o
     * @author  xiaoqianbin
     * @date    2020/6/22
     **/
    public Map<String, Object> toMap(Object o) {
        Map<String, Object> map = new HashMap<>();
        for (Field field : o.getClass().getDeclaredFields()) {
            field.setAccessible(true);
            try {
                Object value = field.get(o);
                if (null != value && !value.getClass().getName().startsWith("java")) {
                    map.put(field.getName(), toMap(value));
                } else {
                    if (value instanceof Date) {
                        JsonFormat jf = field.getDeclaredAnnotation(JsonFormat.class);
                        if (null != jf) {
                            map.put(field.getName(), new SimpleDateFormat(jf.pattern()).format(value));
                        } else {
                            map.put(field.getName(), value);
                        }
                    } else {
                        map.put(field.getName(), value);
                    }
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
        return  map;
    }

    /**
     * 清除bulk请求中的数据，释放出内存
     * @param	bulk
     * @author  xiaoqianbin
     * @date    2020/6/22
     **/
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

    public TransportClient getClient() {
        return client;
    }
}
