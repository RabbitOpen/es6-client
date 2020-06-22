package org.jd.es;

import org.jd.es.service.ElasticSearchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * es controller
 * @author xiaoqianbin
 * @date 2020/6/16
 **/
@RestController
public class EsController {

    private AtomicLong index = new AtomicLong(0);

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    ElasticSearchService ess;

    @RequestMapping("/addBatch")
    public void addBatch(@RequestParam("batch")long batch, @RequestParam(name = "batchNo", defaultValue = "1000")long batchNo) throws InterruptedException {
        long begin = System.currentTimeMillis();
        int count = 10;
        AtomicLong ab = new AtomicLong(batch);
        CountDownLatch cdl = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            new Thread(() -> {
                while (ab.addAndGet(-1) >= 0) {
                    long start = System.currentTimeMillis();
                    List<Long> list = new ArrayList<>();
                    for (long j = 0; j < batchNo; j++) {
                        list.add(index.getAndAdd(1L));
                    }
                    ess.bulkAdd(list);
                    logger.info("{} bulkAdd cost: {}", Thread.currentThread().getName(), System.currentTimeMillis() - start);
                }
                cdl.countDown();
            }).start();
        }
        cdl.await();
        logger.info("current index: [{}], batch cost: {}, fail: {}", index.get(),
                System.currentTimeMillis() - begin, ess.getFailure().get());
    }

    public static void main(String[] args) {
        List<Integer> list = new ArrayList<>();
        for (int i = 100000; i < 100100; i++) {
            list.add(i);

        }
        System.out.println(list);
    }
}
