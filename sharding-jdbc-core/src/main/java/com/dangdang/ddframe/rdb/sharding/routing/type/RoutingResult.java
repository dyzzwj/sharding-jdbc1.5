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

package com.dangdang.ddframe.rdb.sharding.routing.type;

import lombok.Getter;

/**
 * 路由结果.
 * 
 * @author zhangliang
 */
@Getter
public class RoutingResult {

    /**
     * 路由表单元集合
     *  dataSourceName--数据源名称，logicTableName--逻辑表名称，actualTableName--实际表名称，
     *  例如：TableUnit:{dataSourceName:ds_jdbc_1, logicTableName:t_order, actualTableName: t_order_1}
     */
    private final TableUnits tableUnits = new TableUnits();
    
    /**
     * 判断是否为单库表路由.
     *
     * @return 是否为单库表路由
     */
    public boolean isSingleRouting() {
        return 1 == tableUnits.getTableUnits().size();
    }
}
