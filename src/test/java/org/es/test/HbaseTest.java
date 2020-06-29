package org.es.test;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.jd.SpringBootEntry;
import org.jd.hbase.HbaseService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author xiaoqianbin
 * @date 2020/6/28
 **/
@RunWith(SpringRunner.class)
@SpringBootTest(classes = SpringBootEntry.class)
public class HbaseTest {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    HbaseService bs;

    @Test
    public void addTest() throws IOException {
        addData();
        addData();
        addData();
        return;
    }

    private void addData() throws IOException {
        Table table = bs.getConnection().getTable(TableName.valueOf("iou"));
        long start = System.currentTimeMillis();
        ArrayList<Put> puts = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Put put = new Put(Bytes.toBytes("20160628" + "iou" + i));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("user"), Bytes.toBytes("张三" + i));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("iou"), Bytes.toBytes("iou-" + i));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("bizDate"), Bytes.toBytes(new SimpleDateFormat("yyyyMMdd").format(new Date())));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address"), Bytes.toBytes("成都"));
            puts.add(put);
        }
        table.put(puts);
        logger.info("cost: {}", System.currentTimeMillis() - start);
        table.close();
    }

    @Test
    public void getTest() throws IOException, InterruptedException {
        Table table = bs.getConnection().getTable(TableName.valueOf("iou"));
        long start = System.currentTimeMillis();
        List<Get> list = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            Get get = new Get(Bytes.toBytes("20160628iou-" + (1500000 + i)));
            list.add(get);
        }
        Result[] results = new Result[list.size()];
        table.batch(list, results);
        logger.info("cost: {}", System.currentTimeMillis() - start);
        table.close();
        return;
    }
}
