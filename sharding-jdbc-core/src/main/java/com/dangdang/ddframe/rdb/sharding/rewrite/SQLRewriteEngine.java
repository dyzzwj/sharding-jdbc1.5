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

package com.dangdang.ddframe.rdb.sharding.rewrite;


import com.dangdang.ddframe.rdb.sharding.api.rule.BindingTableRule;
import com.dangdang.ddframe.rdb.sharding.api.rule.ShardingRule;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.context.OrderItem;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.context.limit.Limit;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.statement.SQLStatement;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.statement.dql.select.SelectStatement;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.token.ItemsToken;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.token.OffsetToken;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.token.OrderByToken;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.token.RowCountToken;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.token.SQLToken;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.token.TableToken;
import com.dangdang.ddframe.rdb.sharding.routing.type.TableUnit;
import com.dangdang.ddframe.rdb.sharding.routing.type.complex.CartesianTableReference;
import com.google.common.base.Optional;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * SQL重写引擎.
 *
 * @author zhangliang
 */
public final class SQLRewriteEngine {
    
    private final ShardingRule shardingRule;
    /**
     * 原始SQL
     */
    private final String originalSQL;
    /**
     * SQL标记对象
     */
    private final List<SQLToken> sqlTokens = new LinkedList<>();
    /**
     * SQL 语句解析结果对象
     */
    private final SQLStatement sqlStatement;
    
    public SQLRewriteEngine(final ShardingRule shardingRule, final String originalSQL, final SQLStatement sqlStatement) {
        this.shardingRule = shardingRule;
        this.originalSQL = originalSQL;
        this.sqlStatement = sqlStatement;
        sqlTokens.addAll(sqlStatement.getSqlTokens());
    }
    
    /**
     * SQL改写.
     *
     * @param isRewriteLimit 是否重写Limit
     * @return SQL构建器
     */
    public SQLBuilder rewrite(final boolean isRewriteLimit) {
        SQLBuilder result = new SQLBuilder();
        if (sqlTokens.isEmpty()) {
            result.appendLiterals(originalSQL);
            return result;
        }
        int count = 0;
        // 排序SQLToken，按照 beginPosition 递增
        sortByBeginPosition();
        /**
         * SQL改写以 SQLToken 为间隔，顺序改写。
         * 顺序：调用 #sortByBeginPosition() 将 SQLToken 按照 beginPosition 升序。
         * 间隔：遍历 SQLToken，逐个拼接。
         */
        for (SQLToken each : sqlTokens) {
            if (0 == count) { // 拼接第一个 SQLToken 前的字符串
                result.appendLiterals(originalSQL.substring(0, each.getBeginPosition()));
            }
            // 拼接每个SQLToken
            if (each instanceof TableToken) {
                appendTableToken(result, (TableToken) each, count, sqlTokens);
            } else if (each instanceof ItemsToken) {
                appendItemsToken(result, (ItemsToken) each, count, sqlTokens);
            } else if (each instanceof RowCountToken) {
                appendLimitRowCount(result, (RowCountToken) each, count, sqlTokens, isRewriteLimit);
            } else if (each instanceof OffsetToken) {
                appendLimitOffsetToken(result, (OffsetToken) each, count, sqlTokens, isRewriteLimit);
            } else if (each instanceof OrderByToken) {
                appendOrderByToken(result);
            }
            count++;
        }
        return result;
    }
    
    private void sortByBeginPosition() {
        Collections.sort(sqlTokens, new Comparator<SQLToken>() {
            
            @Override
            public int compare(final SQLToken o1, final SQLToken o2) {
                return o1.getBeginPosition() - o2.getBeginPosition();
            }
        });
    }

    /**
     * 拼接 TableToken
     *
     * @param sqlBuilder SQL构建器
     * @param tableToken tableToken
     * @param count tableToken 在 sqlTokens 的顺序
     * @param sqlTokens sqlTokens
     */
    private void appendTableToken(final SQLBuilder sqlBuilder, final TableToken tableToken, final int count, final List<SQLToken> sqlTokens) {
        // 拼接 TableToken
        /**
         * sqlStatement.getTables().getTableNames().contains(tableToken.getTableName()) 目的是处理掉表名前后有的特殊字符，例如SELECT * FROM 't_order' 中 t_order 前后有 ' 符号。
         */
        String tableName = sqlStatement.getTables().getTableNames().contains(tableToken.getTableName()) ? tableToken.getTableName() : tableToken.getOriginalLiterals();
        sqlBuilder.appendTable(tableName);
        // SQLToken 后面的字符串
        int beginPosition = tableToken.getBeginPosition() + tableToken.getOriginalLiterals().length();
        int endPosition = sqlTokens.size() - 1 == count ? originalSQL.length() : sqlTokens.get(count + 1).getBeginPosition();
        sqlBuilder.appendLiterals(originalSQL.substring(beginPosition, endPosition));
    }

    /**
     * 拼接 TableToken
     *
     * @param sqlBuilder SQL构建器
     * @param itemsToken itemsToken
     * @param count itemsToken 在 sqlTokens 的顺序
     * @param sqlTokens sqlTokens
     */
    private void appendItemsToken(final SQLBuilder sqlBuilder, final ItemsToken itemsToken, final int count, final List<SQLToken> sqlTokens) {
        // 拼接 ItemsToken
        for (String item : itemsToken.getItems()) {
            sqlBuilder.appendLiterals(", ");
            sqlBuilder.appendLiterals(item);
        }
        // SQLToken 后面的字符串
        int beginPosition = itemsToken.getBeginPosition();
        int endPosition = sqlTokens.size() - 1 == count ? originalSQL.length() : sqlTokens.get(count + 1).getBeginPosition();
        sqlBuilder.appendLiterals(originalSQL.substring(beginPosition, endPosition));
    }

    /**
     * 拼接 RowCountToken
     *
     * @param sqlBuilder SQL构建器
     * @param rowCountToken rowCountToken
     * @param count count 在 sqlTokens 的顺序
     * @param sqlTokens sqlTokens
     * @param isRewrite 是否重写。当路由结果为单分片时无需重写
     */
    private void appendLimitRowCount(final SQLBuilder sqlBuilder, final RowCountToken rowCountToken, final int count, final List<SQLToken> sqlTokens, final boolean isRewrite) {
        SelectStatement selectStatement = (SelectStatement) sqlStatement;
        Limit limit = selectStatement.getLimit();
        if (!isRewrite) { // 路由结果为单分片
            sqlBuilder.appendLiterals(String.valueOf(rowCountToken.getRowCount()));
            /**
             * [1.1] !selectStatement.getGroupByItems().isEmpty() 跨分片分组需要在内存计算，可能需要全部加载。如果不全部加载，部分结果被分页条件错误结果，会导致结果不正确。
             * [1.2] !selectStatement.getAggregationSelectItems().isEmpty()) 跨分片聚合列需要在内存计算，可能需要全部加载。如果不全部加载，部分结果被分页条件错误结果，会导致结果不正确。
             * [1.1][1.2]，可能变成必须的前提是 GROUP BY 和 ORDER BY 排序不一致。如果一致，各分片已经排序完成，无需内存中排序。
             */
        } else if ((!selectStatement.getGroupByItems().isEmpty() || // [1.1] 跨分片分组需要在内存计算，可能需要全部加载
                !selectStatement.getAggregationSelectItems().isEmpty()) // [1.2] 跨分片聚合列需要在内存计算，可能需要全部加载
                && !selectStatement.isSameGroupByAndOrderByItems()) { // [2] 如果排序一致，即各分片已经排序好结果，就不需要全部加载
            sqlBuilder.appendLiterals(String.valueOf(Integer.MAX_VALUE));
        } else { // 路由结果为多分片
            sqlBuilder.appendLiterals(String.valueOf(limit.isRowCountRewriteFlag() ? rowCountToken.getRowCount() + limit.getOffsetValue() : rowCountToken.getRowCount()));
        }
        // SQLToken 后面的字符串
        int beginPosition = rowCountToken.getBeginPosition() + String.valueOf(rowCountToken.getRowCount()).length();
        int endPosition = sqlTokens.size() - 1 == count ? originalSQL.length() : sqlTokens.get(count + 1).getBeginPosition();
        sqlBuilder.appendLiterals(originalSQL.substring(beginPosition, endPosition));
    }

    /**
     * 拼接 OffsetToken
     *
     * @param sqlBuilder SQL构建器
     * @param offsetToken offsetToken
     * @param count offsetToken 在 sqlTokens 的顺序
     * @param sqlTokens sqlTokens
     * @param isRewrite 是否重写。当路由结果为单分片时无需重写
     */
    private void appendLimitOffsetToken(final SQLBuilder sqlBuilder, final OffsetToken offsetToken, final int count, final List<SQLToken> sqlTokens, final boolean isRewrite) {
        // 拼接 OffsetToken
        /**
         * 当分页跨分片时，需要每个分片都查询后在内存中进行聚合。此时 isRewrite = true。为什么是 "0" 开始呢？每个分片在 [0, offset) 的记录可能属于实际分页结果，因而查询每个分片需要从 0 开始。
         * 当分页单分片时，则无需重写，该分片执行的结果即是最终结果。SQL改写在SQL路由之后就有这个好处。如果先改写，因为没办法知道最终是单分片还是跨分片，考虑正确性，只能统一使用跨分片
         */
        sqlBuilder.appendLiterals(isRewrite ? "0" : String.valueOf(offsetToken.getOffset()));
        // SQLToken 后面的字符串
        int beginPosition = offsetToken.getBeginPosition() + String.valueOf(offsetToken.getOffset()).length();
        int endPosition = sqlTokens.size() - 1 == count ? originalSQL.length() : sqlTokens.get(count + 1).getBeginPosition();
        sqlBuilder.appendLiterals(originalSQL.substring(beginPosition, endPosition));
    }

    /**
     * 拼接 OrderByToken
     *
     * @param sqlBuilder SQL构建器
     */
    private void appendOrderByToken(final SQLBuilder sqlBuilder) {
        /**
         * 数据库里，当无 ORDER BY条件 而有 GROUP BY 条件时候，会使用 GROUP BY条件将结果升序排序：
         *
         * SELECT order_id FROM t_order GROUP BY order_id 等价于 SELECT order_id FROM t_order GROUP BY order_id ORDER BY order_id ASC
         * SELECT order_id FROM t_order GROUP BY order_id DESC 等价于 SELECT order_id FROM t_order GROUP BY order_id ORDER BY order_id DESC
         */
        SelectStatement selectStatement = (SelectStatement) sqlStatement;
        // 拼接 OrderByToken
        StringBuilder orderByLiterals = new StringBuilder(" ORDER BY ");
        int i = 0;
        for (OrderItem each : selectStatement.getOrderByItems()) {
            if (0 == i) {
                orderByLiterals.append(each.getColumnLabel()).append(" ").append(each.getType().name());
            } else {
                orderByLiterals.append(",").append(each.getColumnLabel()).append(" ").append(each.getType().name());
            }
            i++;
        }
        orderByLiterals.append(" ");
        sqlBuilder.appendLiterals(orderByLiterals.toString());
    }
    
    /**
     * 生成SQL语句.
     * 
     * @param tableUnit 路由表单元
     * @param sqlBuilder SQL构建器
     * @return SQL语句
     */
    public String generateSQL(final TableUnit tableUnit, final SQLBuilder sqlBuilder) {
        return sqlBuilder.toSQL(getTableTokens(tableUnit));
    }
    
    /**
     * 生成SQL语句.
     *
     * @param cartesianTableReference 笛卡尔积路由表单元
     * @param sqlBuilder SQL构建器
     * @return SQL语句
     */
    public String generateSQL(final CartesianTableReference cartesianTableReference, final SQLBuilder sqlBuilder) {
        return sqlBuilder.toSQL(getTableTokens(cartesianTableReference));
    }

    /**
     * 获得（路由表单元逻辑表 和 与其互为BindingTable关系的逻辑表）对应的真实表映射（逻辑表需要在 SQL 中存在）
     *
     * @param tableUnit 路由表单元
     * @return 集合
     */
    private Map<String, String> getTableTokens(final TableUnit tableUnit) {
        Map<String, String> tableTokens = new HashMap<>();
        tableTokens.put(tableUnit.getLogicTableName(), tableUnit.getActualTableName());
        Optional<BindingTableRule> bindingTableRule = shardingRule.findBindingTableRule(tableUnit.getLogicTableName());
        // 查找 BindingTableRule
        if (bindingTableRule.isPresent()) {
            tableTokens.putAll(getBindingTableTokens(tableUnit, bindingTableRule.get()));
        }
        return tableTokens;
    }

    /**
     * 获得（笛卡尔积表路由组里的路由表单元逻辑表 和 与其互为BindingTable关系的逻辑表）对应的真实表映射（逻辑表需要在 SQL 中存在）
     *
     * @param cartesianTableReference 笛卡尔积表路由组
     * @return 集合
     */
    private Map<String, String> getTableTokens(final CartesianTableReference cartesianTableReference) {
        Map<String, String> tableTokens = new HashMap<>();
        for (TableUnit each : cartesianTableReference.getTableUnits()) {
            tableTokens.put(each.getLogicTableName(), each.getActualTableName());
            // 查找 BindingTableRule
            Optional<BindingTableRule> bindingTableRule = shardingRule.findBindingTableRule(each.getLogicTableName());
            if (bindingTableRule.isPresent()) {
                tableTokens.putAll(getBindingTableTokens(each, bindingTableRule.get()));
            }
        }
        return tableTokens;
    }

    /**
     * 获得 BindingTable 关系的逻辑表对应的真实表映射（逻辑表需要在 SQL 中存在）
     *
     * @param tableUnit 路由单元
     * @param bindingTableRule Binding表规则配置对象
     * @return 映射
     */
    private Map<String, String> getBindingTableTokens(final TableUnit tableUnit, final BindingTableRule bindingTableRule) {
        Map<String, String> result = new HashMap<>();
        for (String eachTable : sqlStatement.getTables().getTableNames()) {
            if (!eachTable.equalsIgnoreCase(tableUnit.getLogicTableName()) && bindingTableRule.hasLogicTable(eachTable)) {
                result.put(eachTable, bindingTableRule.getBindingActualTable(tableUnit.getDataSourceName(), eachTable, tableUnit.getActualTableName()));
            }
        }
        return result;
    }
}
