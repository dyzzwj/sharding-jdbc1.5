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

package com.dangdang.ddframe.rdb.sharding.jdbc.core.statement;

import com.dangdang.ddframe.rdb.common.base.AbstractShardingJDBCDatabaseAndTableTest;
import com.dangdang.ddframe.rdb.sharding.constant.DatabaseType;
import com.dangdang.ddframe.rdb.sharding.jdbc.core.datasource.ShardingDataSource;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import static junit.framework.TestCase.assertFalse;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

public final class ShardingStatementTest extends AbstractShardingJDBCDatabaseAndTableTest {
    
    private Map<DatabaseType, ShardingDataSource> shardingDataSources;
    
    private String sql = "SELECT COUNT(*) AS orders_count FROM t_order WHERE status = 'init'";
    
    private String sql2 = "DELETE FROM t_order WHERE status ='init'";
    
    private String sql3 = "INSERT INTO t_order_item(order_id, user_id, status) VALUES (%d, %d, '%s')";
    
    @Before
    public void init() throws SQLException {
        shardingDataSources = getShardingDataSources();
    }
    
    @Test
    public void assertExecuteQuery() throws SQLException {
        for (ShardingDataSource each : shardingDataSources.values()) {
            try (
                    Connection connection = each.getConnection();
                    Statement stmt = connection.createStatement();
                    ResultSet resultSet = stmt.executeQuery(sql)) {
                assertTrue(resultSet.next());
                assertThat(resultSet.getLong(1), is(4L));
            }
        }
    }
    
    @Test
    public void assertExecuteUpdate() throws SQLException {
        for (ShardingDataSource each : shardingDataSources.values()) {
            try (
                    Connection connection = each.getConnection();
                    Statement stmt = connection.createStatement()) {
                assertThat(stmt.executeUpdate(sql2), is(4));
            }
        }
    }
    
    @Test
    public void assertExecute() throws SQLException {
        for (ShardingDataSource each : shardingDataSources.values()) {
            try (
                    Connection connection = each.getConnection();
                    Statement stmt = connection.createStatement()) {
                assertTrue(stmt.execute(sql));
                assertTrue(stmt.getResultSet().next());
                assertThat(stmt.getResultSet().getLong(1), is(4L));
            }
        }
    }
    
    @Test
    public void assertExecuteQueryWithResultSetTypeAndResultSetConcurrency() throws SQLException {
        for (ShardingDataSource each : shardingDataSources.values()) {
            try (
                    Connection connection = each.getConnection();
                    Statement stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                    ResultSet resultSet = stmt.executeQuery(sql)) {
                assertTrue(resultSet.next());
                assertThat(resultSet.getLong(1), is(4L));
            }
        }
    }
    
    @Test
    public void assertExecuteQueryWithResultSetTypeAndResultSetConcurrencyAndResultSetHoldability() throws SQLException {
        for (ShardingDataSource each : shardingDataSources.values()) {
            try (
                    Connection connection = each.getConnection();
                    Statement stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
                    ResultSet resultSet = stmt.executeQuery(sql)) {
                assertTrue(resultSet.next());
                assertThat(resultSet.getLong(1), is(4L));
            }
        }
    }
    
    @Test
    public void assertExecuteUpdateWithAutoGeneratedKeys() throws SQLException {
        for (ShardingDataSource each : shardingDataSources.values()) {
            try (
                    Connection connection = each.getConnection();
                    Statement stmt = connection.createStatement()) {
                assertThat(stmt.executeUpdate(sql2, Statement.NO_GENERATED_KEYS), is(4));
            }
        }
    }
    
    @Test
    public void assertExecuteUpdateWithColumnIndexes() throws SQLException {
        for (Map.Entry<DatabaseType, ShardingDataSource> each : shardingDataSources.entrySet()) {
            if (DatabaseType.PostgreSQL != each.getKey()) {
                try (
                        Connection connection = each.getValue().getConnection();
                        Statement stmt = connection.createStatement()) {
                    assertThat(stmt.executeUpdate(sql2, new int[]{1}), is(4));
                }
            }
        }
    }
    
    @Test
    public void assertExecuteUpdateWithColumnNames() throws SQLException {
        for (Map.Entry<DatabaseType, ShardingDataSource> each : shardingDataSources.entrySet()) {
            if (DatabaseType.H2 == each.getKey() || DatabaseType.MySQL == each.getKey()) {
                try (
                        Connection connection = each.getValue().getConnection();
                        Statement stmt = connection.createStatement()) {
                    assertThat(stmt.executeUpdate(sql2, new String[]{"orders_count"}), is(4));
                }
            }
        }
    }
    
    @Test
    public void assertExecuteWithAutoGeneratedKeys() throws SQLException {
        for (ShardingDataSource each : shardingDataSources.values()) {
            try (
                    Connection connection = each.getConnection();
                    Statement stmt = connection.createStatement()) {
                assertTrue(stmt.execute(sql, Statement.NO_GENERATED_KEYS));
                assertTrue(stmt.getResultSet().next());
                assertThat(stmt.getResultSet().getLong(1), is(4L));
            }
        }
    }
    
    @Test
    public void assertExecuteWithColumnIndexes() throws SQLException {
        for (Map.Entry<DatabaseType, ShardingDataSource> each : shardingDataSources.entrySet()) {
            if (DatabaseType.PostgreSQL != each.getKey()) {
                try (
                        Connection connection = each.getValue().getConnection();
                        Statement stmt = connection.createStatement()) {
                    assertTrue(stmt.execute(sql, new int[]{1}));
                    assertTrue(stmt.getResultSet().next());
                    assertThat(stmt.getResultSet().getLong(1), is(4L));
                }
            }
        }
    }
    
    @Test
    public void assertExecuteWithColumnNames() throws SQLException {
        for (Map.Entry<DatabaseType, ShardingDataSource> each : shardingDataSources.entrySet()) {
            if (DatabaseType.PostgreSQL != each.getKey()) {
                try (
                        Connection connection = each.getValue().getConnection();
                        Statement stmt = connection.createStatement()) {
                    assertTrue(stmt.execute(sql, new String[]{"orders_count"}));
                    assertTrue(stmt.getResultSet().next());
                    assertThat(stmt.getResultSet().getLong(1), is(4L));
                }
            }
        }
    }
    
    @Test
    public void assertGetConnection() throws SQLException {
        for (ShardingDataSource each : shardingDataSources.values()) {
            try (
                    Connection connection = each.getConnection();
                    Statement stmt = connection.createStatement()) {
                assertThat(stmt.getConnection(), is(connection));
            }
        }
    }
    
    @Test
    public void assertGetGeneratedKeys() throws SQLException {
        for (Map.Entry<DatabaseType, ShardingDataSource> each : shardingDataSources.entrySet()) {
            if (DatabaseType.PostgreSQL != each.getKey()) {
                try (
                        Connection connection = each.getValue().getConnection();
                        Statement stmt = connection.createStatement()) {
                    assertFalse(stmt.execute(String.format(sql3, 1, 1, "init")));
                    if (DatabaseType.MySQL != each.getKey() && DatabaseType.SQLServer != each.getKey()) {
                        assertFalse(stmt.getGeneratedKeys().next());
                    }
                    assertFalse(stmt.execute(String.format(sql3, 1, 1, "init"), Statement.NO_GENERATED_KEYS));
                    if (DatabaseType.MySQL != each.getKey() && DatabaseType.SQLServer != each.getKey()) {
                        assertFalse(stmt.getGeneratedKeys().next());
                    }
                    assertFalse(stmt.execute(String.format(sql3, 1, 1, "init"), Statement.RETURN_GENERATED_KEYS));
                    ResultSet generatedKeysResultSet = stmt.getGeneratedKeys();
                    assertTrue(generatedKeysResultSet.next());
                    assertThat(generatedKeysResultSet.getLong(1), is(3L));
                    assertFalse(stmt.execute(String.format(sql3, 1, 1, "init"), new int[]{1}));
                    generatedKeysResultSet = stmt.getGeneratedKeys();
                    assertTrue(generatedKeysResultSet.next());
                    assertThat(generatedKeysResultSet.getLong(1), is(4L));
                    assertFalse(stmt.execute(String.format(sql3, 1, 1, "init"), new String[]{"user_id"}));
                    generatedKeysResultSet = stmt.getGeneratedKeys();
                    assertTrue(generatedKeysResultSet.next());
                    assertThat(generatedKeysResultSet.getLong(1), is(5L));
                    assertFalse(stmt.execute(String.format(sql3, 1, 1, "init"), new int[]{2}));
                    generatedKeysResultSet = stmt.getGeneratedKeys();
                    assertTrue(generatedKeysResultSet.next());
                    assertThat(generatedKeysResultSet.getLong(1), is(6L));
                    assertFalse(stmt.execute(String.format(sql3, 1, 1, "init"), new String[]{"no"}));
                    generatedKeysResultSet = stmt.getGeneratedKeys();
                    assertTrue(generatedKeysResultSet.next());
                    assertThat(generatedKeysResultSet.getLong(1), is(7L));
                }
            }
        }
    }
}
