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

package com.dangdang.ddframe.rdb.sharding.routing.type.complex;

import com.dangdang.ddframe.rdb.sharding.api.rule.BindingTableRule;
import com.dangdang.ddframe.rdb.sharding.api.rule.ShardingRule;
import com.dangdang.ddframe.rdb.sharding.api.rule.TableRule;
import com.dangdang.ddframe.rdb.sharding.exception.ShardingJdbcException;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.statement.SQLStatement;
import com.dangdang.ddframe.rdb.sharding.routing.type.RoutingResult;
import com.dangdang.ddframe.rdb.sharding.routing.type.RoutingEngine;
import com.dangdang.ddframe.rdb.sharding.routing.type.simple.SimpleRoutingEngine;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;

/**
 * 混合多库表路由引擎.
 * ComplexRoutingEngine 根据路由结果会转化成 SimpleRoutingEngine 或 ComplexRoutingEngine
 *
 */
@RequiredArgsConstructor
@Slf4j
public final class ComplexRoutingEngine implements RoutingEngine {
    
    private final ShardingRule shardingRule;
    
    private final List<Object> parameters;
    
    private final Collection<String> logicTables;
    
    private final SQLStatement sqlStatement;

    /**
     * ComplexRoutingEngine 计算每个逻辑表的简单路由分片，路由结果交给 CartesianRoutingEngine 继续路由形成笛卡尔积结果。
     * @return
     */
    @Override
    public RoutingResult route() {
        Collection<RoutingResult> result = new ArrayList<>(logicTables.size());
        Collection<String> bindingTableNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        // 计算每个逻辑表的简单路由分片
        for (String each : logicTables) {
            Optional<TableRule> tableRule = shardingRule.tryFindTableRule(each);
            if (tableRule.isPresent()) {
                if (!bindingTableNames.contains(each)) {
                    /**
                     *  计算每个逻辑表的简单路由分片
                     */
                    result.add(new SimpleRoutingEngine(shardingRule, parameters, tableRule.get().getLogicTable(), sqlStatement).route());
                }
                // 互为 BindingTable 关系的表加到 bindingTableNames 里，不重复计算分片
                Optional<BindingTableRule> bindingTableRule = shardingRule.findBindingTableRule(each);
                if (bindingTableRule.isPresent()) {
                    bindingTableNames.addAll(Lists.transform(bindingTableRule.get().getTableRules(), new Function<TableRule, String>() {
                        
                        @Override
                        public String apply(final TableRule input) {
                            return input.getLogicTable();
                        }
                    }));
                }
            }
        }
        log.trace("mixed tables sharding result: {}", result);
        if (result.isEmpty()) {
            throw new ShardingJdbcException("Cannot find table rule and default data source with logic tables: '%s'", logicTables);
        }
        // 防御性编程。shardingRule#isAllBindingTables() 已经过滤了这个情况。
        if (1 == result.size()) {
            return result.iterator().next();
        }
        // 交给 CartesianRoutingEngine 形成笛卡尔积结果
        return new CartesianRoutingEngine(result).route();
    }

}