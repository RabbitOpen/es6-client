package org.jd.hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author xiaoqianbin
 * @date 2020/6/28
 **/
@RestController
@RequestMapping("/hbase")
public class HbaseController {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private AtomicLong index = new AtomicLong(0);

    @Resource
    HbaseService bs;

    @RequestMapping("/addBatch")
    public void addBatch(@RequestParam(name = "batch", defaultValue = "1") int batch,
                         @RequestParam(name = "batchSize", defaultValue = "1000") int batchSize) throws InterruptedException {
        long start = System.currentTimeMillis();
        CountDownLatch cdl = new CountDownLatch(10);
        for (int j = 0; j < 10; j++) {
            new Thread(() -> {
                for (int i = 0; i < batch; i++) {
                    try {
                        addData(batchSize);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                cdl.countDown();
            }).start();
        }
        cdl.await();
        logger.info("total cost: {}", System.currentTimeMillis() - start);
    }

    @RequestMapping("/get")
    public Map<String, String> get(@RequestParam(name = "id") String id) throws IOException {
        long start = System.currentTimeMillis();
        Table iou = bs.getConnection().getTable(TableName.valueOf("iou"));
        Result result = iou.get(new Get(Bytes.toBytes(id)));
        NavigableMap<byte[], byte[]> info = result.getFamilyMap(Bytes.toBytes("info"));
        iou.close();
        Map<String, String> map = new HashMap<>();
        for (byte[] bytes : info.keySet()) {
            map.put(Bytes.toString(bytes), Bytes.toString(info.get(bytes)));
        }
        logger.info("cost: {}", System.currentTimeMillis() - start);
        return map;
    }

    @RequestMapping("/scan")
    public void scan() throws IOException {
        long begin = System.currentTimeMillis();
        HbaseService bs = new HbaseService();
        bs.init();
        Table table = bs.getConnection().getTable(TableName.valueOf("iou"));
        ResultScanner scanner = table.getScanner(Bytes.toBytes("info"));
        long count = 0;
        while (true) {
            long start = System.currentTimeMillis();
            Result[] next = scanner.next(1000);
            count += next.length;
            logger.info("size:{}, total:{}, cost: {}", next.length, count, System.currentTimeMillis() - start);
            if (next.length < 1000) {
                break;
            }
        }
        table.close();
        logger.info("total cost: {}, scan count: {}", System.currentTimeMillis() - begin, count);
    }

    @RequestMapping("/batchGet")
    public Map<String, String> batchGet() throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        Table table = bs.getConnection().getTable(TableName.valueOf("iou"));
        List<Get> list = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            Get get = new Get(Bytes.toBytes("20160628iou-" + (1400000 + i)));
            list.add(get);
        }
        Result[] results = new Result[list.size()];
        table.batch(list, results);
        table.close();
        logger.info("cost: {}", System.currentTimeMillis() - start);
        return null;
    }


    public void addData(int batch) throws IOException {
        Table table = bs.getConnection().getTable(TableName.valueOf("iou"));
        long start = System.currentTimeMillis();
        ArrayList<Put> puts = new ArrayList<>();
        for (int i = 0; i < batch; i++) {
            Put put = new Put(Bytes.toBytes("20200628" + String.format("%08d")));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("bizDate"), Bytes.toBytes(new SimpleDateFormat("yyyyMMdd").format(new Date())));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("iou"), Bytes.toBytes("iou-" + i));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("user"), Bytes.toBytes("张三" + i));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address"), Bytes.toBytes("成都"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address1"), Bytes.toBytes("成都市青羊区苏坡乡金沙十年"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address2"), Bytes.toBytes("成都市青羊区苏坡乡金沙十年"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address3"), Bytes.toBytes("成都市青羊区苏坡乡金沙十年"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address4"), Bytes.toBytes("成都市青羊区苏坡乡金沙十年"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address5"), Bytes.toBytes("成都市青羊区苏坡乡金沙十年"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address6"), Bytes.toBytes("成都市青羊区苏坡乡金沙十年"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address7"), Bytes.toBytes("成都市青羊区苏坡乡金沙十年"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address8"), Bytes.toBytes("成都市青羊区苏坡乡金沙十年"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address9"), Bytes.toBytes("成都市青羊区苏坡乡金沙十年"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address10"), Bytes.toBytes("成都市青羊区苏坡乡金沙十年"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address11"), Bytes.toBytes("成都市青羊区苏坡乡金沙十年"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address12"), Bytes.toBytes("成都市青羊区苏坡乡金沙十年"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address13"), Bytes.toBytes("成都市青羊区苏坡乡金沙十年"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address14"), Bytes.toBytes("成都市青羊区苏坡乡金沙十年"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address15"), Bytes.toBytes("成都市青羊区苏坡乡金沙十年"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address16"), Bytes.toBytes("成都市青羊区苏坡乡金沙十年"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address17"), Bytes.toBytes("成都市青羊区苏坡乡金沙十年"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address18"), Bytes.toBytes("成都市青羊区苏坡乡金沙十年"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address19"), Bytes.toBytes("成都市青羊区苏坡乡金沙十年"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address20"), Bytes.toBytes("成都市青羊区苏坡乡金沙十年"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address21"), Bytes.toBytes("成都市青羊区苏坡乡金沙十年"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address22"), Bytes.toBytes("成都市青羊区苏坡乡金沙十年"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address23"), Bytes.toBytes("成都市青羊区苏坡乡金沙十年"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address24"), Bytes.toBytes("成都市青羊区苏坡乡金沙十年"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address25"), Bytes.toBytes("成都市青羊区苏坡乡金沙十年"));
            puts.add(put);
        }
        table.put(puts);
        logger.info("cost: {}", System.currentTimeMillis() - start);
        table.close();
    }
}
