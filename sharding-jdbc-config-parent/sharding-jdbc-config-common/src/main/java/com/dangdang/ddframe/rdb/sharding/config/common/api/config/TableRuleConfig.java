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

import java.util.ArrayList;
import java.util.List;

/**
 * 表规则配置.
 * 
 * @author gaohongtao
 */
@Getter
@Setter
public class TableRuleConfig {
    
    private boolean dynamic;

    private String actualTables;
    
    private String dataSourceNames;

    //分库策略
    private StrategyConfig databaseStrategy;

    //分表策略
    private StrategyConfig tableStrategy;

    /**
     * 主键字段名及其生成策略
     */
    private List<GenerateKeyColumnConfig> generateKeyColumns = new ArrayList<>();
}
