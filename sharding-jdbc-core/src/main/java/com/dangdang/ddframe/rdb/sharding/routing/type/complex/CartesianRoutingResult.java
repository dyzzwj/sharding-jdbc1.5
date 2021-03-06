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

import com.dangdang.ddframe.rdb.sharding.routing.type.RoutingResult;
import lombok.Getter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * 笛卡尔积路由结果.
 * 
 * @author gaohongtao
 * @author zhangliang
 */
@ToString
public final class CartesianRoutingResult extends RoutingResult {

    /**
     *  routingDataSources
     *  每个元素 对应的是不同的数据源
     */
    @Getter
    private final List<CartesianDataSource> routingDataSources = new ArrayList<>();
    
    void merge(final String dataSource, final Collection<CartesianTableReference> routingTableReferences) {
        for (CartesianTableReference each : routingTableReferences) {
            //each:属于某个逻辑表的真实表集合组对应的TableUnit集合组
            merge(dataSource, each);
        }
        System.out.println(routingDataSources);
    }
    
    private void merge(final String dataSource, final CartesianTableReference routingTableReference) {
        for (CartesianDataSource each : routingDataSources) {
            //数据源相同
            if (each.getDataSource().equalsIgnoreCase(dataSource)) {
                /**
                 * 把相同数据源的routingTableReference
                 */
                each.getRoutingTableReferences().add(routingTableReference);
                return;
            }
        }

        //不同数据源
        routingDataSources.add(new CartesianDataSource(dataSource, routingTableReference));
    }
    
    @Override
    public boolean isSingleRouting() {
        Collection<CartesianTableReference> cartesianTableReferences = new LinkedList<>();
        for (CartesianDataSource cartesianDataSource : routingDataSources) {
            for (CartesianTableReference cartesianTableReference : cartesianDataSource.getRoutingTableReferences()) {
                cartesianTableReferences.add(cartesianTableReference);
            }
        }
        return 1 == cartesianTableReferences.size();
    }
}
