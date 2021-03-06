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

import lombok.Getter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 笛卡尔积路由数据源.
 * 
 * @author zhangliang
 */
@Getter
@ToString
public final class CartesianDataSource {

    /**
     * 数据源
     */
    private final String dataSource;

    /**
     *  相同数据源的属于某个逻辑表的真实表集合组对应的TableUnit集合组
     *  1、数据源相同
     *  2、每个元素CartesianTableReference代表数据某个逻辑表的真实表集合组对应的TableUnit集合组
     *
     */
    private final List<CartesianTableReference> routingTableReferences;
    
    CartesianDataSource(final String dataSource, final CartesianTableReference routingTableReference) {
        this.dataSource = dataSource;
        routingTableReferences = new ArrayList<>(Collections.singletonList(routingTableReference));
    }
}
