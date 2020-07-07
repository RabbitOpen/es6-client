package org.jd.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;

/**
 * @author xiaoqianbin
 * @date 2020/6/25
 **/
@Service
public class HbaseService {

    private Logger logger = LoggerFactory.getLogger(getClass());

    Connection connection;

//    @PostConstruct
    public void init() throws IOException {
//        System.setProperty("hadoop.home.dir", "D:\\Program Files\\hadoop-2.8.5");
        Configuration config = new Configuration();
        config.set("hbase.zookeeper.quorum", "10.222.16.82");// zookeeper地址
        config.set("hbase.zookeeper.property.clientPort", "2181");// zookeeper端口
        connection = ConnectionFactory.createConnection(config);
    }

    public Connection getConnection() {
        return connection;
    }

}
