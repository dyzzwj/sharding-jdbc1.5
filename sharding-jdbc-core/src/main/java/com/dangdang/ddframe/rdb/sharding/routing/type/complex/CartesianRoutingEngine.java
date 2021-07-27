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

import com.dangdang.ddframe.rdb.sharding.routing.type.RoutingEngine;
import com.dangdang.ddframe.rdb.sharding.routing.type.RoutingResult;
import com.dangdang.ddframe.rdb.sharding.routing.type.TableUnit;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * 笛卡尔积的库表路由. 同库 才可以进行笛卡尔积
 * 
 * @author zhangliang
 */
@RequiredArgsConstructor
@Slf4j
public final class CartesianRoutingEngine implements RoutingEngine {

    /**
     * ComplexRoutingEngine 计算出的每个逻辑表的简单路由分片，
     * routingResults[0] 是t_order表的简单路由分片
     * routingResults[1] 是t_order_item表的简单路由分片
     *
     *
     *   db1.t_order_1              db1.t_order_1
     *   db1.t_order_2              db1.t_order_2
     *   db2.t_order_1              db2.t_order_1
     *   db2.t_order_2              db2.t_order_2
     *
     */
    private final Collection<RoutingResult> routingResults;


    @Override
    public CartesianRoutingResult route() {
        CartesianRoutingResult result = new CartesianRoutingResult();
        /**
         * getDataSourceLogicTablesMap: 获得同库对应的逻辑表集合 Entry<数据源（库）, Set<逻辑表>> entry。
         * ds1 - [t_order,t_order_item]
         */
        for (Entry<String, Set<String>> entry : getDataSourceLogicTablesMap().entrySet()) { // Entry<数据源（库）, Set<逻辑表>> entry
            // 根据数据源和逻辑表名称获取真实表集合组
            List<Set<String>> actualTableGroups = getActualTableGroups(entry.getKey(), entry.getValue()); // List<Set<真实表>>
            /**
             *
             *  遍历数据源（库），获得当前数据源（库）的路由表单元分组。
             *
             * List<Set<String>> actualTableGroups: 每个元素是当前数据源 逻辑表对应的真实表集合组
             * List<Set<TableUnit>> tableUnitGroups:每个元素是当前数据源 属于某个逻辑表的真实表集合组对应的TableUnit集合组
             *
             *   actualTableGroups和tableUnitGroups相同索引是一一对应的
             *   actualTableGroups： 0 - [t_order_0,t_order_1]
             *                      1 - [t_order_item_0,t_order_item_1]
             *
             *   tableUnitGroups   0 - [{TableUnit(dataSourceName=ds_jdbc_1, logicTableName=t_order, actualTableName=t_order_0)},{TableUnit(dataSourceName=ds_jdbc_1, logicTableName=t_order, actualTableName=t_order_1)}]
             *                     1 - [{TableUnit(dataSourceName=ds_jdbc_1, logicTableName=t_order_item, actualTableName=t_order_item_1)},{TableUnit(dataSourceName=ds_jdbc_1, logicTableName=t_order_item, actualTableName=t_order_item_0)}]
             *
             */
            List<Set<TableUnit>> tableUnitGroups = toTableUnitGroups(entry.getKey(), actualTableGroups);

            /**
             *  笛卡尔积，并合并结果
             *  同库 才可以进行笛卡尔积
             *  计算笛卡尔积
             *  Sets.cartesianProduct(tableUnitGroups):
             *           0 - [{TableUnit(dataSourceName=ds_jdbc_1, logicTableName=t_order, actualTableName=t_order_0)},{TableUnit(dataSourceName=ds_jdbc_1, logicTableName=t_order_item, actualTableName=t_order_item_1)}]
             *         1 - [{TableUnit(dataSourceName=ds_jdbc_1, logicTableName=t_order, actualTableName=t_order_0)},{TableUnit(dataSourceName=ds_jdbc_1, logicTableName=t_order_item, actualTableName=t_order_item_0)}]
             *         2 - [{TableUnit(dataSourceName=ds_jdbc_1, logicTableName=t_order, actualTableName=t_order_1)},{TableUnit(dataSourceName=ds_jdbc_1, logicTableName=t_order_item, actualTableName=t_order_item_1)}]
             *         3 - [{TableUnit(dataSourceName=ds_jdbc_1, logicTableName=t_order, actualTableName=t_order_1)},{TableUnit(dataSourceName=ds_jdbc_1, logicTableName=t_order_item, actualTableName=t_order_item_0)}]
             *
             */
            result.merge(entry.getKey(), getCartesianTableReferences(Sets.cartesianProduct(tableUnitGroups)));
        }
        log.trace("cartesian tables sharding result: {}", result);
        return result;
    }

    /**
     * 获得同库对应的逻辑表集合
     *
     * @return 同库对应的逻辑表集合
     */
    private Map<String, Set<String>> getDataSourceLogicTablesMap() {
        //获得所有路由结果里的数据源（库）交集 ds1,ds2
        Collection<String> intersectionDataSources = getIntersectionDataSources();
        Map<String, Set<String>> result = new HashMap<>(routingResults.size());
        // 获得同库对应的逻辑表集合
        for (RoutingResult each : routingResults) {
            /**
             * each.getTableUnits().getDataSourceLogicTablesMap:获取数据源和逻辑表名称集合的映射关系.
             *  k - 数据源名称
             *  v - 该数据源下的逻辑表集合
             *  ds1 - [t_order,t_order_item]
             */
            for (Entry<String, Set<String>> entry : each.getTableUnits().getDataSourceLogicTablesMap(intersectionDataSources).entrySet()) { // 过滤掉不在数据源（库）交集的逻辑表

                if (result.containsKey(entry.getKey())) {
                    result.get(entry.getKey()).addAll(entry.getValue());
                } else {
                    result.put(entry.getKey(), entry.getValue());
                }
            }
        }
        return result;
    }

    /**
     * 获得所有路由结果里的数据源（库）交集
     *
     * @return 数据源（库）交集
     */
    private Collection<String> getIntersectionDataSources() {
        Collection<String> result = new HashSet<>();
        for (RoutingResult each : routingResults) {
            if (result.isEmpty()) {
                result.addAll(each.getTableUnits().getDataSourceNames());
            }
            //取交集
            result.retainAll(each.getTableUnits().getDataSourceNames()); // 交集
        }
        return result;
    }


    private List<Set<String>> getActualTableGroups(final String dataSource, final Set<String> logicTables) {
        List<Set<String>> result = new ArrayList<>(logicTables.size());
        for (RoutingResult each : routingResults) {
            result.addAll(each.getTableUnits().getActualTableNameGroups(dataSource, logicTables));
        }
        return result;
    }
    
    private List<Set<TableUnit>> toTableUnitGroups(final String dataSource, final List<Set<String>> actualTableGroups) {
        List<Set<TableUnit>> result = new ArrayList<>(actualTableGroups.size());
        for (Set<String> each : actualTableGroups) {
            //根据数据源和真实表名查找TableUnit

            result.add(new HashSet<>(Lists.transform(new ArrayList<>(each), new Function<String, TableUnit>() {
    
                @Override
                public TableUnit apply(final String input) {
                    return findTableUnit(dataSource, input);
                }
            })));
        }
        return result;
    }
    
    private TableUnit findTableUnit(final String dataSource, final String actualTable) {
        for (RoutingResult each : routingResults) {
            Optional<TableUnit> result = each.getTableUnits().findTableUnit(dataSource, actualTable);
            if (result.isPresent()) {
                return result.get();
            }
        }
        throw new IllegalStateException(String.format("Cannot found routing table factor, data source: %s, actual table: %s", dataSource, actualTable));
    }
    
    private List<CartesianTableReference> getCartesianTableReferences(final Set<List<TableUnit>> cartesianTableUnitGroups) {

        /**
         *  笛卡尔积，并合并结果
         *  同库 才可以进行笛卡尔积
         *  计算笛卡尔积
         * cartesianTableUnitGroups：
         *         0 - [{TableUnit(dataSourceName=ds_jdbc_1, logicTableName=t_order, actualTableName=t_order_0)},{TableUnit(dataSourceName=ds_jdbc_1, logicTableName=t_order_item, actualTableName=t_order_item_1)}]
         *         1 - [{TableUnit(dataSourceName=ds_jdbc_1, logicTableName=t_order, actualTableName=t_order_0)},{TableUnit(dataSourceName=ds_jdbc_1, logicTableName=t_order_item, actualTableName=t_order_item_0)}]
         *         2 - [{TableUnit(dataSourceName=ds_jdbc_1, logicTableName=t_order, actualTableName=t_order_1)},{TableUnit(dataSourceName=ds_jdbc_1, logicTableName=t_order_item, actualTableName=t_order_item_1)}]
         *         3 - [{TableUnit(dataSourceName=ds_jdbc_1, logicTableName=t_order, actualTableName=t_order_1)},{TableUnit(dataSourceName=ds_jdbc_1, logicTableName=t_order_item, actualTableName=t_order_item_0)}]
         *
         */
        List<CartesianTableReference> result = new ArrayList<>(cartesianTableUnitGroups.size());

        for (List<TableUnit> each : cartesianTableUnitGroups) {
            //CartesianTableReference:[{TableUnit(dataSourceName=ds_jdbc_1, logicTableName=t_order, actualTableName=t_order_0)},{TableUnit(dataSourceName=ds_jdbc_1, logicTableName=t_order_item, actualTableName=t_order_item_1)}]
            result.add(new CartesianTableReference(each));
        }
        return result;
    }
}
