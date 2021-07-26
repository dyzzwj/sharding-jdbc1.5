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

package com.dangdang.ddframe.rdb.sharding.routing.type.simple;

import com.dangdang.ddframe.rdb.sharding.api.ShardingValue;
import com.dangdang.ddframe.rdb.sharding.api.rule.DataNode;
import com.dangdang.ddframe.rdb.sharding.api.rule.ShardingRule;
import com.dangdang.ddframe.rdb.sharding.api.rule.TableRule;
import com.dangdang.ddframe.rdb.sharding.api.strategy.database.DatabaseShardingStrategy;
import com.dangdang.ddframe.rdb.sharding.api.strategy.table.TableShardingStrategy;
import com.dangdang.ddframe.rdb.sharding.hint.HintManagerHolder;
import com.dangdang.ddframe.rdb.sharding.hint.ShardingKey;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.context.condition.Column;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.context.condition.Condition;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.statement.SQLStatement;
import com.dangdang.ddframe.rdb.sharding.routing.type.RoutingEngine;
import com.dangdang.ddframe.rdb.sharding.routing.type.RoutingResult;
import com.dangdang.ddframe.rdb.sharding.routing.type.TableUnit;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * 简单路由引擎.
 * 
 * @author zhangliang
 */
@RequiredArgsConstructor
public final class SimpleRoutingEngine implements RoutingEngine {
    
    private final ShardingRule shardingRule;
    
    private final List<Object> parameters;
    
    private final String logicTableName;
    
    private final SQLStatement sqlStatement;


    /**
     * 执行SQL："SELECT o.* FROM t_order o where o.user_id=? AND o.order_id=?"时，由于SQL中只有一个表（1 == tableNames.size()），所以路由引擎是SimpleRoutingEngine
     * @return
     */
    @Override
    public RoutingResult route() {
        /**
         *   根据逻辑表得到tableRule，逻辑表为t_order；表规则的配置为：.actualTables(Arrays.asList("t_order_0", "t_order_1"))，所以有两个实际表；
         */
        TableRule tableRule = shardingRule.getTableRule(logicTableName);
        // 根据规则先路由数据源：即根据user_id取模路由
        Collection<String> routedDataSources = routeDataSources(tableRule);
        Map<String, Collection<String>> routedMap = new LinkedHashMap<>(routedDataSources.size());
        /**
         * 遍历路由到的目标数据源
         */
        for (String each : routedDataSources) {
            /**
             *  // 再根据规则路由表：即根据order_id取模路由
             */
            routedMap.put(each, routeTables(tableRule, each));
        }

        /**
         *   将得到的路由数据源和表信息封装到RoutingResult中，RoutingResult中有个TableUnits类型属性，
         *   TableUnits类中有个List<TableUnit> tableUnits属性，TableUnit包含三个属性：
         *   dataSourceName--数据源名称，logicTableName--逻辑表名称，actualTableName--实际表名称，
         *   例如：TableUnit:{dataSourceName:ds_jdbc_1, logicTableName:t_order, actualTableName: t_order_1}
         */
        return generateRoutingResult(tableRule, routedMap);
    }
    
    private Collection<String> routeDataSources(final TableRule tableRule) {
        /**
         * 拿到分库策略
         */
        DatabaseShardingStrategy strategy = shardingRule.getDatabaseShardingStrategy(tableRule);
        //可以使用 HintManager 设置库分片值进行强制路由。
        /**
         * 获取分片值 分片键对应的value   以计算该value对应的库
         */
        List<ShardingValue<?>> shardingValues = HintManagerHolder.isUseShardingHint() ? getDatabaseShardingValuesFromHint(strategy.getShardingColumns())
                : getShardingValues(strategy.getShardingColumns());
        /**
         * 静态路由     计算静态分片  返回分库后指向的数据源集合
         */
        Collection<String> result = strategy.doStaticSharding(tableRule.getActualDatasourceNames(), shardingValues);
        Preconditions.checkState(!result.isEmpty(), "no database route info");
        return result;
    }
    
    private Collection<String> routeTables(final TableRule tableRule, final String routedDataSource) {
        /**
         * 获取表分片策略
         */
        TableShardingStrategy strategy = shardingRule.getTableShardingStrategy(tableRule);

        /**
         *    获取分片值 分片键对应的value   以计算该value对应的表
         */
        List<ShardingValue<?>> shardingValues = HintManagerHolder.isUseShardingHint() ? getTableShardingValuesFromHint(strategy.getShardingColumns())
                : getShardingValues(strategy.getShardingColumns());

        /**
         *   计算分片 返回分表后的指向的表集合
         */
        Collection<String> result = tableRule.isDynamic() ? strategy.doDynamicSharding(shardingValues) : strategy.doStaticSharding(tableRule.getActualTableNames(routedDataSource), shardingValues);
        Preconditions.checkState(!result.isEmpty(), "no table route info");
        return result;
    }
    
    private List<ShardingValue<?>> getDatabaseShardingValuesFromHint(final Collection<String> shardingColumns) {
        List<ShardingValue<?>> result = new ArrayList<>(shardingColumns.size());
        for (String each : shardingColumns) {
            Optional<ShardingValue<?>> shardingValue = HintManagerHolder.getDatabaseShardingValue(new ShardingKey(logicTableName, each));
            if (shardingValue.isPresent()) {
                result.add(shardingValue.get());
            }
        }
        return result;
    }
    
    private List<ShardingValue<?>> getTableShardingValuesFromHint(final Collection<String> shardingColumns) {
        List<ShardingValue<?>> result = new ArrayList<>(shardingColumns.size());
        for (String each : shardingColumns) {
            Optional<ShardingValue<?>> shardingValue = HintManagerHolder.getTableShardingValue(new ShardingKey(logicTableName, each));
            if (shardingValue.isPresent()) {
                result.add(shardingValue.get());
            }
        }
        return result;
    }
    
    private List<ShardingValue<?>> getShardingValues(final Collection<String> shardingColumns) {
        List<ShardingValue<?>> result = new ArrayList<>(shardingColumns.size());
        //遍历分片列
        for (String each : shardingColumns) {
            /**
             * Condition 里只放明确影响路由的条件，例如：order_id = 1, order_id IN (1, 2), order_id BETWEEN (1, 3)，
             * 不放无法计算的条件，例如：o.order_id = i.order_id。该方法里，使用分片键从 Condition 查找 分片值
             */
            Optional<Condition> condition = sqlStatement.getConditions().find(new Column(each, logicTableName));
            if (condition.isPresent()) {
                result.add(condition.get().getShardingValue(parameters));
            }
        }
        return result;
    }
    
    private RoutingResult generateRoutingResult(final TableRule tableRule, final Map<String, Collection<String>> routedMap) {
        RoutingResult result = new RoutingResult();
        for (Entry<String, Collection<String>> entry : routedMap.entrySet()) {
            //根据数据源名称过滤获取真实数据单元.
            Collection<DataNode> dataNodes = tableRule.getActualDataNodes(entry.getKey(), entry.getValue());
            for (DataNode each : dataNodes) {
                result.getTableUnits().getTableUnits().add(new TableUnit(each.getDataSourceName(), logicTableName, each.getTableName()));
            }
        }
        return result;
    }
}
