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

package com.dangdang.ddframe.rdb.sharding.routing.router;

import com.codahale.metrics.Timer.Context;
import com.dangdang.ddframe.rdb.sharding.api.rule.ShardingRule;
import com.dangdang.ddframe.rdb.sharding.jdbc.core.ShardingContext;
import com.dangdang.ddframe.rdb.sharding.metrics.MetricsContext;
import com.dangdang.ddframe.rdb.sharding.parsing.SQLJudgeEngine;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.statement.SQLStatement;
import com.dangdang.ddframe.rdb.sharding.routing.SQLExecutionUnit;
import com.dangdang.ddframe.rdb.sharding.routing.SQLRouteResult;
import com.dangdang.ddframe.rdb.sharding.routing.type.RoutingResult;
import com.dangdang.ddframe.rdb.sharding.routing.type.TableUnit;
import com.dangdang.ddframe.rdb.sharding.routing.type.hint.DatabaseHintRoutingEngine;
import com.dangdang.ddframe.rdb.sharding.util.SQLLogger;

import java.util.List;

/**
 * 通过提示且仅路由至数据库的SQL路由器.
 * 
 * @author zhangiang
 */
public final class DatabaseHintSQLRouter implements SQLRouter {

    /**
     * 分片规则
     */
    private final ShardingRule shardingRule;
    
    private final boolean showSQL;
    
    public DatabaseHintSQLRouter(final ShardingContext shardingContext) {
        shardingRule = shardingContext.getShardingRule();
        showSQL = shardingContext.isShowSQL();
    }


    /**
     * sql解析
     * @param logicSQL 逻辑SQL
     * @param parametersSize 参数个数
     * @return
     */
    @Override
    public SQLStatement parse(final String logicSQL, final int parametersSize) {
        /**
         *  只解析 SQL 类型 即 SELECT / UPDATE / DELETE / INSERT
         */
        return new SQLJudgeEngine(logicSQL).judge();
    }
    
    @Override
    // TODO insert的SQL仍然需要解析自增主键
    /**
     * 通过提示且仅路由至数据库的SQL路由器
     * 目前不支持 Sharding-JDBC 的主键自增
     */
    public SQLRouteResult route(final String logicSQL, final List<Object> parameters, final SQLStatement sqlStatement) {
        Context context = MetricsContext.start("Route SQL");
        SQLRouteResult result = new SQLRouteResult(sqlStatement);
        // 路由
        //使用的分库策略来自 ShardingRule，不是 TableRule 因为 SQL 未解析表名。因此，即使在 TableRule 设置了 actualTables 属性也是没有效果的。
        RoutingResult routingResult = new DatabaseHintRoutingEngine(shardingRule.getDataSourceRule(), shardingRule.getDatabaseShardingStrategy())
                .route();
        // SQL最小执行单元
        for (TableUnit each : routingResult.getTableUnits().getTableUnits()) {
            /**
             * HintManager.getInstance().setDatabaseShardingValue(库分片值) 设置的库分片值使用的是 EQUALS，
             * 因而分库策略计算出来的只有一个库分片，即 TableUnit 只有一个，SQLExecutionUnit 只有一个
             */
            result.getExecutionUnits().add(new SQLExecutionUnit(each.getDataSourceName(), logicSQL));
        }
        MetricsContext.stop(context);
        if (showSQL) {
            SQLLogger.logSQL(logicSQL, sqlStatement, result.getExecutionUnits(), parameters);
        }
        return result;
    }

}
