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
import com.dangdang.ddframe.rdb.sharding.constant.DatabaseType;
import com.dangdang.ddframe.rdb.sharding.jdbc.core.ShardingContext;
import com.dangdang.ddframe.rdb.sharding.metrics.MetricsContext;
import com.dangdang.ddframe.rdb.sharding.parsing.SQLParsingEngine;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.context.GeneratedKey;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.statement.SQLStatement;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.token.RowCountToken;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.statement.dml.insert.InsertStatement;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.statement.dql.select.SelectStatement;
import com.dangdang.ddframe.rdb.sharding.rewrite.SQLBuilder;
import com.dangdang.ddframe.rdb.sharding.rewrite.SQLRewriteEngine;
import com.dangdang.ddframe.rdb.sharding.routing.SQLExecutionUnit;
import com.dangdang.ddframe.rdb.sharding.routing.SQLRouteResult;
import com.dangdang.ddframe.rdb.sharding.routing.type.RoutingEngine;
import com.dangdang.ddframe.rdb.sharding.routing.type.RoutingResult;
import com.dangdang.ddframe.rdb.sharding.routing.type.TableUnit;
import com.dangdang.ddframe.rdb.sharding.routing.type.complex.CartesianDataSource;
import com.dangdang.ddframe.rdb.sharding.routing.type.complex.CartesianRoutingResult;
import com.dangdang.ddframe.rdb.sharding.routing.type.complex.CartesianTableReference;
import com.dangdang.ddframe.rdb.sharding.routing.type.complex.ComplexRoutingEngine;
import com.dangdang.ddframe.rdb.sharding.routing.type.simple.SimpleRoutingEngine;
import com.dangdang.ddframe.rdb.sharding.util.SQLLogger;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * 需要解析的SQL路由器.
 * 
 * @author zhangiang
 */
public final class ParsingSQLRouter implements SQLRouter {

    //分片规则
    private final ShardingRule shardingRule;

    //数据库类型
    private final DatabaseType databaseType;

    //是否显示sql
    private final boolean showSQL;
    
    private final List<Number> generatedKeys;
    
    public ParsingSQLRouter(final ShardingContext shardingContext) {
        shardingRule = shardingContext.getShardingRule();
        databaseType = shardingContext.getDatabaseType();
        showSQL = shardingContext.isShowSQL();
        generatedKeys = new LinkedList<>();
    }

    /**
     * sql解析
     * @param logicSQL 逻辑SQL
     * @param parametersSize 参数个数
     * @return
     */
    @Override
    public SQLStatement parse(final String logicSQL, final int parametersSize) {
        //构建sql解析引擎
        SQLParsingEngine parsingEngine = new SQLParsingEngine(databaseType, logicSQL, shardingRule);
        Context context = MetricsContext.start("Parse SQL");
        //解析sql
        SQLStatement result = parsingEngine.parse();
        if (result instanceof InsertStatement) { // 处理 GenerateKeyToken
            // 如果是insert操作，需要额外对主键做处理
            ((InsertStatement) result).appendGenerateKeyToken(shardingRule, parametersSize);
        }
        MetricsContext.stop(context);
        return result;
    }

    /**
     * 需要解析的SQL路由器
     */
    @Override
    public SQLRouteResult route(final String logicSQL, final List<Object> parameters, final SQLStatement sqlStatement) {
        final Context context = MetricsContext.start("Route SQL");
        SQLRouteResult result = new SQLRouteResult(sqlStatement);
        // 处理 插入SQL 主键字段
        if (sqlStatement instanceof InsertStatement && null != ((InsertStatement) sqlStatement).getGeneratedKey()) {
            processGeneratedKey(parameters, (InsertStatement) sqlStatement, result);
        }
        /**
         * 路由
         */
        RoutingResult routingResult = route(parameters, sqlStatement);
        // SQL重写引擎
        SQLRewriteEngine rewriteEngine = new SQLRewriteEngine(shardingRule, logicSQL, sqlStatement);
        boolean isSingleRouting = routingResult.isSingleRouting();
        // 处理分页
        if (sqlStatement instanceof SelectStatement && null != ((SelectStatement) sqlStatement).getLimit()) {
            processLimit(parameters, (SelectStatement) sqlStatement, isSingleRouting);
        }
        // SQL 重写
        SQLBuilder sqlBuilder = rewriteEngine.rewrite(!isSingleRouting);
        // 生成 ExecutionUnit
        /**
         * 对于笛卡尔积路由结果和简单路由结果传递的参数略有不同：前者使用 CartesianDataSource ( CartesianTableReference )，后者使用路由表单元 ( TableUnit )
         */
        if (routingResult instanceof CartesianRoutingResult) {
            for (CartesianDataSource cartesianDataSource : ((CartesianRoutingResult) routingResult).getRoutingDataSources()) {
                for (CartesianTableReference cartesianTableReference : cartesianDataSource.getRoutingTableReferences()) {
                    result.getExecutionUnits().add(new SQLExecutionUnit(cartesianDataSource.getDataSource(), rewriteEngine.generateSQL(cartesianTableReference, sqlBuilder))); // 生成 SQL
                }
            }
        } else {
            for (TableUnit each : routingResult.getTableUnits().getTableUnits()) {
                result.getExecutionUnits().add(new SQLExecutionUnit(each.getDataSourceName(), rewriteEngine.generateSQL(each, sqlBuilder))); // 生成 SQL
            }
        }
        MetricsContext.stop(context);
        // 打印 SQL
        if (showSQL) {
            SQLLogger.logSQL(logicSQL, sqlStatement, result.getExecutionUnits(), parameters);
        }
        return result;
    }

    /**
     * 根据表情况使用 SimpleRoutingEngine 或 CartesianRoutingEngine
     *
     * @param parameters 占位参数
     * @param sqlStatement SQL语句对象
     * @return 路由结果
     */
    private RoutingResult route(final List<Object> parameters, final SQLStatement sqlStatement) {
        Collection<String> tableNames = sqlStatement.getTables().getTableNames();
        RoutingEngine routingEngine;
        /**
         *  如果sql中只有一个表名，或者多个表名之间是绑定表关系，或者所有表都在默认数据源指定的数据库中（即不参与分库分表的表），
         *  那么用SimpleRoutingEngine作为路由判断引擎；多表互为BindingTable关系时，每张表的路由结果是相同的，所以只要计算第一张表的分片即可。
         */
        if (1 == tableNames.size() || shardingRule.isAllBindingTables(tableNames)) {
            /**
             * tableNames.iterator().next() 注意下，tableNames 变量是 new TreeSet<>(String.CASE_INSENSITIVE_ORDER)。
             * 所以 SELECT * FROM t_order o join t_order_item i ON o.order_id = i.order_id 即使 t_order_item 排在 t_order 前面，
             * tableNames.iterator().next() 返回的是 t_order。当 t_order 和 t_order_item 为 BindingTable关系 时，计算的是 t_order 路由分片
             * TreeSet的排序
             */
            routingEngine = new SimpleRoutingEngine(shardingRule, parameters, tableNames.iterator().next(), sqlStatement);
        } else {
            // TODO 可配置是否执行笛卡尔积
            routingEngine = new ComplexRoutingEngine(shardingRule, parameters, tableNames, sqlStatement);
        }
        return routingEngine.route();
    }

    /**
     * 处理 插入SQL 主键字段
     * 当 主键编号 未生成时，{@link ShardingRule#generateKey(String)} 进行生成
     *
     * @param parameters 占位符参数
     * @param insertStatement Insert SQL语句对象
     * @param sqlRouteResult SQL路由结果
     */
    private void processGeneratedKey(final List<Object> parameters, final InsertStatement insertStatement, final SQLRouteResult sqlRouteResult) {
        GeneratedKey generatedKey = insertStatement.getGeneratedKey();
        if (parameters.isEmpty()) { // 已有主键，无占位符，INSERT INTO t_order(order_id, user_id) VALUES (1, 100);
            sqlRouteResult.getGeneratedKeys().add(generatedKey.getValue());
        } else if (parameters.size() == generatedKey.getIndex()) { // 主键字段不存在存在，INSERT INTO t_order(user_id) VALUES(?);
            Number key = shardingRule.generateKey(insertStatement.getTables().getSingleTableName()); // 生成主键编号
            parameters.add(key);
            setGeneratedKeys(sqlRouteResult, key);
        } else if (-1 != generatedKey.getIndex()) { // 主键字段存在，INSERT INTO t_order(order_id, user_id) VALUES(?, ?);
            setGeneratedKeys(sqlRouteResult, (Number) parameters.get(generatedKey.getIndex()));
        }
    }

    /**
     * 设置 主键编号 到 SQL路由结果
     *
     * @param sqlRouteResult SQL路由结果
     * @param generatedKey 主键编号
     */
    private void setGeneratedKeys(final SQLRouteResult sqlRouteResult, final Number generatedKey) {
        generatedKeys.add(generatedKey);
        sqlRouteResult.getGeneratedKeys().clear();
        sqlRouteResult.getGeneratedKeys().addAll(generatedKeys);
    }

    /**
     * 处理分页条件
     *
     * @see SQLRewriteEngine#appendLimitRowCount(SQLBuilder, RowCountToken, int, List, boolean)
     * @param parameters 占位符对应参数列表
     * @param selectStatement Select SQL语句对象
     * @param isSingleRouting 是否单表路由
     */
    private void processLimit(final List<Object> parameters, final SelectStatement selectStatement, final boolean isSingleRouting) {
        boolean isNeedFetchAll = (!selectStatement.getGroupByItems().isEmpty() // // [1.1] 跨分片分组需要在内存计算，可能需要全部加载
                                    || !selectStatement.getAggregationSelectItems().isEmpty()) // [1.2] 跨分片聚合列需要在内存计算，可能需要全部加载
                                && !selectStatement.isSameGroupByAndOrderByItems(); // [2] 如果排序一致，即各分片已经排序好结果，就不需要全部加载
        selectStatement.getLimit().processParameters(parameters, !isSingleRouting, isNeedFetchAll);
    }

}
