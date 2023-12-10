package com.zzjj.consistency.config;

import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

/**
 * 请求TemplateBean
 *
 * @author zengjin
 * @date 2023/11/19
 **/
@Component
public class RestTemplateConfig {

    // 支持http请求和通信的组件
    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

}
