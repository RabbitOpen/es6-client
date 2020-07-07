package org.jd;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 *
 * @author xiaoqianbin
 * @date 2020/6/4
 **/
@SpringBootApplication
public class SpringBootEntry {

    public static void main(String[] args) {
        SpringApplication.run(SpringBootEntry.class);

        // 不占端口启动
//        SpringApplication app = new SpringApplication(SpringBootEntry.class);
//        app.setWebApplicationType(WebApplicationType.NONE);
//        app.run(args);
    }

}
