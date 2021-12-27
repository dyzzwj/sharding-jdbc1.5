package com.dyzwj.zwjsharding;

import org.apache.ibatis.type.LocalDateTimeTypeHandler;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class ZwjShardingApplication {

    public static void main(String[] args) {
        SpringApplication.run(ZwjShardingApplication.class, args);
    }

}
