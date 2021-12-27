/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package com.dangdang.ddframe.rdb.sharding.config.common.api.config;

import lombok.Getter;
import lombok.Setter;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 分片规则配置.
 * 
 * @author gaohongtao
 */
@Getter
@Setter
public class ShardingRuleConfig {

    /**
     * 数据源集合
     */
    private Map<String, DataSource> dataSource = new HashMap<>();

    //默认数据源名
    private String defaultDataSourceName;

    /**
     * 表规则配置
     */
    private Map<String, TableRuleConfig> tables = new HashMap<>();

    //绑定表规则配置
    private List<BindingTableRuleConfig> bindingTables = new ArrayList<>();


    private StrategyConfig defaultDatabaseStrategy;
    
    private StrategyConfig defaultTableStrategy;

    /**
     * 默认主键生成策略类名
     */
    private String keyGeneratorClass;
}
