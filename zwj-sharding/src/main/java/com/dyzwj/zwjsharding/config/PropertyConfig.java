package com.dyzwj.zwjsharding.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
//@PropertySource(value = "classpath:application-standard.properties")
//@PropertySource(value = "classpath:application-inline.properties")
//@PropertySource(value = "classpath:application-complex.properties")
@PropertySource(value = "classpath:application-hint.properties")
public class PropertyConfig {


}
