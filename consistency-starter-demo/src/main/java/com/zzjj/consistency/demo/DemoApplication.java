package com.zzjj.consistency.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.zzjj.consistency.annotation.EnableTendConsistencyTask;

/**
 * @author zengjin
 * @date 2023/12/10 12:25
 **/
@EnableTendConsistencyTask
@EnableScheduling
@SpringBootApplication
public class DemoApplication {
    public static void main(final String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }
}
