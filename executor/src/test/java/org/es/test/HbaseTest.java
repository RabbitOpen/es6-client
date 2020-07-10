package org.es.test;

import net.sf.json.JSONArray;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.jd.SpringBootEntry;
import org.jd.es.entity.User;
import org.jd.hbase.HbaseService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.NavigableMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

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

    @Test
    public void filterTest() throws IOException, InterruptedException {
        Table table = bs.getConnection().getTable(TableName.valueOf("iou"));
        long start = System.currentTimeMillis();
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("info"));
        scan.setFilter(new RowFilter(CompareOperator.EQUAL, new BinaryPrefixComparator("202006280000100".getBytes())));
        scan.withStartRow("2020062800001000".getBytes());
        scan.withStopRow("2020062800002000".getBytes());
        ResultScanner scanner = table.getScanner(scan);
        Result[] next = scanner.next(100);

        scanner.close();
        logger.info("cost: {}, size: {}", System.currentTimeMillis() - start, next.length);
        table.close();
        return;
    }

    @Test
    public void gzipTest() throws Exception {
        Table table = bs.getConnection().getTable(TableName.valueOf("user"));
        Put put = new Put(Bytes.toBytes("r1"));

        List<User> list = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            User user = new User();
            user.setAge(10);
            user.setName("zhangsan");
            user.setAddress("成都市青羊区");
            list.add(user);
        }
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("plan"), compress(JSONArray.fromObject(list).toString().getBytes()));
        table.put(put);

        Get get = new Get(Bytes.toBytes("r1"));
        Result result = table.get(get);
        NavigableMap<byte[], byte[]> map = result.getFamilyMap(Bytes.toBytes("info"));
        byte[] plans = map.get(Bytes.toBytes("plan"));
        byte[] decompress = decompress(plans);
        String string = new String(decompress);
        list = JSONArray.toList(JSONArray.fromObject(string), User.class);
        table.close();
    }

    public static byte[] compress(byte[] data) throws Exception {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            // 压缩
            compress(bais, baos);
            byte[] output = baos.toByteArray();
            baos.flush();
            return output;
        } finally {
            try {
                baos.close();
            } catch (IOException e) {
                throw e;
            }
            try {
                bais.close();
            } catch (IOException e) {
                throw e;
            }
        }
    }



    /**
     * 数据压缩
     *
     * @param is
     * @param os
     * @throws Exception
     */
    public static void compress(InputStream is, OutputStream os) throws Exception {
        GZIPOutputStream gos = new GZIPOutputStream(os);
        try {
            int count;
            byte data[] = new byte[2048];
            while ((count = is.read(data, 0, 2048)) != -1) {
                gos.write(data, 0, count);
            }
            gos.finish();
            gos.flush();
        } finally {
            gos.close();
        }
    }

    public static byte[] decompress(byte[] data) throws Exception {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            // 解压缩
            decompress(bais, baos);
            data = baos.toByteArray();
            baos.flush();
        } finally {
            try {
                baos.close();
            } catch (IOException e) {
                throw e;
            }
            try {
                bais.close();
            } catch (IOException e) {
                throw e;
            }
        }
        return data;
    }

    public static void decompress(InputStream is, OutputStream os) throws Exception {
        GZIPInputStream gis = new GZIPInputStream(is);
        try {
            int count;
            byte data[] = new byte[2048];
            while ((count = gis.read(data, 0, 2048)) != -1) {
                os.write(data, 0, count);
            }
        } finally {
            try {
                gis.close();
            } catch (IOException e) {
                throw e;
            }
        }
    }
}
