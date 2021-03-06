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

package com.dangdang.ddframe.rdb.sharding.routing.type.hint;

import com.dangdang.ddframe.rdb.sharding.api.ShardingValue;
import com.dangdang.ddframe.rdb.sharding.api.rule.DataSourceRule;
import com.dangdang.ddframe.rdb.sharding.api.strategy.database.DatabaseShardingStrategy;
import com.dangdang.ddframe.rdb.sharding.hint.HintManagerHolder;
import com.dangdang.ddframe.rdb.sharding.hint.ShardingKey;
import com.dangdang.ddframe.rdb.sharding.routing.type.RoutingEngine;
import com.dangdang.ddframe.rdb.sharding.routing.type.RoutingResult;
import com.dangdang.ddframe.rdb.sharding.routing.type.TableUnit;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.Collections;

/**
 * 基于数据库提示的路由引擎.
 * 
 * @author gaohongtao
 * @author zhangliang
 */
@RequiredArgsConstructor
@Slf4j
public final class DatabaseHintRoutingEngine implements RoutingEngine {

    /**
     * 数据源配置对象
     */
    private final DataSourceRule dataSourceRule;
    /**
     * 分库策略
     */
    private final DatabaseShardingStrategy databaseShardingStrategy;

    @Override
    public RoutingResult route() {
        // 从 Hint 获得 分片键值
        Optional<ShardingValue<?>> shardingValue = HintManagerHolder.getDatabaseShardingValue(new ShardingKey(HintManagerHolder.DB_TABLE_NAME, HintManagerHolder.DB_COLUMN_NAME));
        Preconditions.checkState(shardingValue.isPresent());
        log.debug("Before database sharding only db:{} sharding values: {}", dataSourceRule.getDataSourceNames(), shardingValue.get());
        // 路由。表分片规则使用的是 ShardingRule 里的。因为没 SQL 解析。
        Collection<String> routingDataSources = databaseShardingStrategy.doStaticSharding(dataSourceRule.getDataSourceNames(), Collections.<ShardingValue<?>>singleton(shardingValue.get()));
        Preconditions.checkState(!routingDataSources.isEmpty(), "no database route info");
        log.debug("After database sharding only result: {}", routingDataSources);
        // 路由结果
        RoutingResult result = new RoutingResult();
        for (String each : routingDataSources) {
            //new TableUnit(each, "", "") 的 logicTableName，actualTableName 都是空串   无分表操作
            result.getTableUnits().getTableUnits().add(new TableUnit(each, "", ""));
        }
        return result;
    }

}
