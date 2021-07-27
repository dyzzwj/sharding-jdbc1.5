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

package com.dangdang.ddframe.rdb.sharding.executor;

import com.dangdang.ddframe.rdb.sharding.constant.SQLType;
import com.dangdang.ddframe.rdb.sharding.exception.ShardingJdbcException;
import com.dangdang.ddframe.rdb.sharding.executor.event.AbstractExecutionEvent;
import com.dangdang.ddframe.rdb.sharding.executor.event.DMLExecutionEvent;
import com.dangdang.ddframe.rdb.sharding.executor.event.DQLExecutionEvent;
import com.dangdang.ddframe.rdb.sharding.executor.event.EventExecutionType;
import com.dangdang.ddframe.rdb.sharding.executor.threadlocal.ExecutorDataMap;
import com.dangdang.ddframe.rdb.sharding.executor.threadlocal.ExecutorExceptionHandler;
import com.dangdang.ddframe.rdb.sharding.executor.type.batch.BatchPreparedStatementUnit;
import com.dangdang.ddframe.rdb.sharding.executor.type.prepared.PreparedStatementUnit;
import com.dangdang.ddframe.rdb.sharding.executor.type.statement.StatementUnit;
import com.dangdang.ddframe.rdb.sharding.util.EventBusInstance;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * SQL执行引擎.
 *
 *  分表分库，需要执行的 SQL 数量从单条变成了多条，此时有两种方式执行：
 *
 * 串行执行 SQL
 * 并行执行 SQL
 * 前者，编码容易，性能较差，总耗时是多条 SQL 执行时间累加。
 * 后者，编码复杂，性能较好，总耗时约等于执行时间最长的 SQL。
 *
 * 👼 ExecutorEngine 当然采用的是后者，并行执行 SQL。
 *
 *
 * @author gaohongtao
 * @author zhangliang
 */
@Slf4j
public final class ExecutorEngine implements AutoCloseable {
    
    private final ListeningExecutorService executorService;
    
    public ExecutorEngine(final int executorSize) {
        executorService = MoreExecutors.listeningDecorator(new ThreadPoolExecutor(
                executorSize, executorSize, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(),
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ShardingJDBC-%d").build()));
        MoreExecutors.addDelayedShutdownHook(executorService, 60, TimeUnit.SECONDS); // TODO 疑问：会执行到么？数据
    }
    
    /**
     * 执行Statement.
     *
     * @param sqlType SQL类型
     * @param statementUnits 语句对象执行单元集合
     * @param executeCallback 执行回调函数
     * @param <T> 返回值类型
     * @return 执行结果
     */
    public <T> List<T> executeStatement(final SQLType sqlType, final Collection<StatementUnit> statementUnits, final ExecuteCallback<T> executeCallback) {
        return execute(sqlType, statementUnits, Collections.<List<Object>>emptyList(), executeCallback);
    }
    
    /**
     * 执行PreparedStatement.
     *
     * @param sqlType SQL类型
     * @param preparedStatementUnits 语句对象执行单元集合
     * @param parameters 参数列表
     * @param executeCallback 执行回调函数
     * @param <T> 返回值类型
     * @return 执行结果
     */
    public <T> List<T> executePreparedStatement(
            final SQLType sqlType, final Collection<PreparedStatementUnit> preparedStatementUnits, final List<Object> parameters, final ExecuteCallback<T> executeCallback) {
        /**
         *     // preparedStatementUnits就是前面路由分析结果：执行SQL select o.* from t_order o where o.user_id=10时，只需在ds_jdbc_0这个数据源中的t_order_0和t_order_1两个实际表中执行即可；
         */
        return execute(sqlType, preparedStatementUnits, Collections.singletonList(parameters), executeCallback);
    }
    
    /**
     * 执行Batch.
     *
     * @param sqlType SQL类型
     * @param batchPreparedStatementUnits 语句对象执行单元集合
     * @param parameterSets 参数列表集
     * @param executeCallback 执行回调函数
     * @return 执行结果
     */
    public List<int[]> executeBatch(
            final SQLType sqlType, final Collection<BatchPreparedStatementUnit> batchPreparedStatementUnits, final List<List<Object>> parameterSets, final ExecuteCallback<int[]> executeCallback) {
        return execute(sqlType, batchPreparedStatementUnits, parameterSets, executeCallback);
    }

    /**
     * 执行
     *
     * @param sqlType SQL 类型
     * @param baseStatementUnits 语句对象执行单元集合
     * @param parameterSets 参数列表集
     * @param executeCallback 执行回调函数
     * @param <T> 返回值类型
     * @return 执行结果
     */
    private  <T> List<T> execute(
            final SQLType sqlType, final Collection<? extends BaseStatementUnit> baseStatementUnits, final List<List<Object>> parameterSets, final ExecuteCallback<T> executeCallback) {
        if (baseStatementUnits.isEmpty()) {
            return Collections.emptyList();
        }
        Iterator<? extends BaseStatementUnit> iterator = baseStatementUnits.iterator();
        //第一个任务分离出来
        BaseStatementUnit firstInput = iterator.next();
        // 第二个任务开始所有 SQL任务 提交线程池【异步】执行任务
        ListenableFuture<List<T>> restFutures = asyncExecute(sqlType, Lists.newArrayList(iterator), parameterSets, executeCallback);
        T firstOutput;
        List<T> restOutputs;
        try {
            /**
             *  第一个任务【同步】执行任务 [猜测是不是考虑到分库分表后只需路由到一个数据源中的一个表的SQL执行性能问题，优化这种SQL执行为同步执行？
             *  分库分表后，面向用户的API占用了99%的请求量，而这些API对应的SQL 99%只需要在一个数据源上的一个实际表执行即可，例如根据订单表根据user_id分库分表后，查询用户的订单信息这种场景]
             */
            firstOutput = syncExecute(sqlType, firstInput, parameterSets, executeCallback);
            // 等待第二个任务开始所有 SQL任务完成
            restOutputs = restFutures.get();
            //CHECKSTYLE:OFF
        } catch (final Exception ex) {
            //CHECKSTYLE:ON
            ExecutorExceptionHandler.handleException(ex);
            return null;
        }
        // 返回结果
        List<T> result = Lists.newLinkedList(restOutputs);
        //将第一个任务同步执行结果与其他任务异步执行结果合并就是最终的结果
        result.add(0, firstOutput);
        return result;
    }

    private <T> ListenableFuture<List<T>> asyncExecute(
            final SQLType sqlType, final Collection<BaseStatementUnit> baseStatementUnits, final List<List<Object>> parameterSets, final ExecuteCallback<T> executeCallback) {
        List<ListenableFuture<T>> result = new ArrayList<>(baseStatementUnits.size());
        final boolean isExceptionThrown = ExecutorExceptionHandler.isExceptionThrown();
        final Map<String, Object> dataMap = ExecutorDataMap.getDataMap();
        for (final BaseStatementUnit each : baseStatementUnits) {
            // 提交线程池【异步】执行任务
            result.add(executorService.submit(new Callable<T>() {

                @Override
                public T call() throws Exception {
                    return executeInternal(sqlType, each, parameterSets, executeCallback, isExceptionThrown, dataMap);
                }
            }));
        }
        // 返回 ListenableFuture
        return Futures.allAsList(result);
    }

    private <T> T syncExecute(final SQLType sqlType, final BaseStatementUnit baseStatementUnit, final List<List<Object>> parameterSets, final ExecuteCallback<T> executeCallback) throws Exception {
        // 【同步】执行任务
        return executeInternal(sqlType, baseStatementUnit, parameterSets, executeCallback, ExecutorExceptionHandler.isExceptionThrown(), ExecutorDataMap.getDataMap());
    }
    
    private <T> T executeInternal(final SQLType sqlType, final BaseStatementUnit baseStatementUnit, final List<List<Object>> parameterSets, final ExecuteCallback<T> executeCallback, 
                          final boolean isExceptionThrown, final Map<String, Object> dataMap) throws Exception {
        synchronized (baseStatementUnit.getStatement().getConnection()) { // 可能拿到相同的 Connection，同步避免冲突
//            System.out.println(baseStatementUnit.getStatement().getConnection() + "\t" + baseStatementUnit.getSqlExecutionUnit().getSql());

            T result;
            ExecutorExceptionHandler.setExceptionThrown(isExceptionThrown);
            ExecutorDataMap.setDataMap(dataMap);
            List<AbstractExecutionEvent> events = new LinkedList<>();
            // 生成 Event
            if (parameterSets.isEmpty()) {
                events.add(getExecutionEvent(sqlType, baseStatementUnit, Collections.emptyList()));
            } else {
                for (List<Object> each : parameterSets) {
                    events.add(getExecutionEvent(sqlType, baseStatementUnit, each));
                }
            }
            // EventBus 发布 EventExecutionType.BEFORE_EXECUTE
            for (AbstractExecutionEvent event : events) {
                EventBusInstance.getInstance().post(event);
            }
            try {
                // 执行回调函数
                result = executeCallback.execute(baseStatementUnit);
            } catch (final SQLException ex) {
                // EventBus 发布 EventExecutionType.EXECUTE_FAILURE
                for (AbstractExecutionEvent each : events) {
                    each.setEventExecutionType(EventExecutionType.EXECUTE_FAILURE);
                    each.setException(Optional.of(ex));
                    EventBusInstance.getInstance().post(each);
                    ExecutorExceptionHandler.handleException(ex); // TODO 疑问：如果多个任务，只发一个执行异常么？
                }
                return null;
            }
            // EventBus 发布 EventExecutionType.EXECUTE_SUCCESS
            for (AbstractExecutionEvent each : events) {
                each.setEventExecutionType(EventExecutionType.EXECUTE_SUCCESS);
                EventBusInstance.getInstance().post(each);
            }
            return result;
        }
    }
    
    private AbstractExecutionEvent getExecutionEvent(final SQLType sqlType, final BaseStatementUnit baseStatementUnit, final List<Object> parameters) {
        AbstractExecutionEvent result;
        if (SQLType.DQL == sqlType) {
            result = new DQLExecutionEvent(baseStatementUnit.getSqlExecutionUnit().getDataSource(), baseStatementUnit.getSqlExecutionUnit().getSql(), parameters);
        } else {
            result = new DMLExecutionEvent(baseStatementUnit.getSqlExecutionUnit().getDataSource(), baseStatementUnit.getSqlExecutionUnit().getSql(), parameters);
        }
        return result;
    }
    
    @Override
    public void close() {
        executorService.shutdownNow();
        try {
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (final InterruptedException ignored) {
        }
        if (!executorService.isTerminated()) {
            throw new ShardingJdbcException("ExecutorEngine can not been terminated");
        }
    }
}
