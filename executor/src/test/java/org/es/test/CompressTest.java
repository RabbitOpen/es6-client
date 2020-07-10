package org.es.test;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * @author xiaoqianbin
 * @date 2020/7/10
 **/
@RunWith(JUnit4.class)
public class CompressTest {

    @Test
    public void compressTest() throws Exception {

        String t = "abcd1234567890abcd1234567890abcd1234567890abcd1234567890abcd1234567890abcd1234567890abcd1234567890abcd1234567890abcd1234567890abcd1234567890abcd1234567890abcd1234567890abcd1234567890abcd1234567890abcd1234567890abcd1234567890abcd1234567890";
        String text = "";
        for (int i = 0; i < 100; i++) {
            text += t;
        }
        byte[] compress = HbaseTest.compress(text.getBytes());
        System.out.println("text length :" + text.length() + ", bytes: " + compress.length);
        byte[] bytes = HbaseTest.decompress(compress);



        long start = System.currentTimeMillis();


        for (int i = 0; i < 10000; i++) {
            compress = HbaseTest.compress(text.getBytes());
            bytes = HbaseTest.decompress(compress);
        }
        System.out.println(System.currentTimeMillis() - start);
    }
}
