/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sqoop.mapreduce.db;

import java.io.IOException;
import java.sql.*;
import java.util.Arrays;
import java.util.Queue;

import com.cloudera.sqoop.mapreduce.db.DataDrivenDBInputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.sqoop.mapreduce.DBWritable;
import org.apache.hadoop.util.ReflectionUtils;

import com.cloudera.sqoop.mapreduce.db.DBConfiguration;
import com.cloudera.sqoop.mapreduce.db.DBInputFormat;
import org.apache.sqoop.util.LoggingUtils;

/**
 * A RecordReader that reads records from a SQL table.
 * Emits LongWritables containing the record number as
 * key and DBWritables as value.
 */
public class ShardDBRecordReader<T extends DBWritable> extends
        RecordReader<LongWritable, T> {

    private static final Log LOG = LogFactory.getLog(ShardDBRecordReader.class);

    private Class<T> inputClass;

    private Configuration conf;

    private ShardDBInputFormat.ShardDBInputSplit split;

    private long pos = 0;

    private LongWritable key = null;

    private T value = null;

    private Queue<String> connections;
    // used to show process
    private int totalConnectionsSize;

    private ResultSet currentResults = null;

    private PreparedStatement currentStatement;

    private Connection currentConnection;

    private DBConfiguration dbConf;

    private String conditions;

    private String[] fieldNames;

    private String tableName;

    private String dbProductName; // database manufacturer string.

    /**
     * @param split The InputSplit to read data for
     * @throws SQLException
     */
    // CHECKSTYLE:OFF
    // TODO (aaron): Refactor constructor to take fewer arguments
    public ShardDBRecordReader(InputSplit split,
                               Class<T> inputClass, Configuration conf,
                               DBConfiguration dbConfig, String cond, String[] fields, String table,
                               String dbProduct) {
        this.inputClass = inputClass;
        this.split = (ShardDBInputFormat.ShardDBInputSplit) split;
        this.conf = conf;
        this.connections = this.split.getConnections();
        this.totalConnectionsSize = connections.size();
        this.dbConf = dbConfig;
        this.conditions = cond;
        if (fields != null) {
            this.fieldNames = Arrays.copyOf(fields, fields.length);
        }
        this.tableName = table;
        this.dbProductName = dbProduct;
    }
    // CHECKSTYLE:ON

    protected ResultSet executeQuery(String query, Connection connection) throws SQLException {
        this.currentStatement = connection.prepareStatement(query,
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

        Integer fetchSize = dbConf.getFetchSize();
        if (fetchSize != null) {
            LOG.debug("Using fetchSize for next query: " + fetchSize);
            currentStatement.setFetchSize(fetchSize);
        }
        LOG.info("Executing query: " + query);
        return currentStatement.executeQuery();
    }

    /**
     * Returns the query for selecting the records,
     * subclasses can override this for custom behaviour.
     */
    private String getSelectQuery() {
        return getSelectQuery(split.getLowerClause(), split.getUpperClause());
    }

    private String getSelectQuery(String lowerClause, String upperClause) {
        StringBuilder query = new StringBuilder();

        // Build the WHERE clauses associated with the data split first.
        // We need them in both branches of this function.
        StringBuilder conditionClauses = new StringBuilder();
        conditionClauses.append("( ").append(lowerClause);
        conditionClauses.append(" ) AND ( ").append(upperClause);
        conditionClauses.append(" )");

        if (dbConf.getInputQuery() == null) {
            // We need to generate the entire query.
            query.append("SELECT ");

            for (int i = 0; i < fieldNames.length; i++) {
                query.append(fieldNames[i]);
                if (i != fieldNames.length - 1) {
                    query.append(", ");
                }
            }

            query.append(" FROM ").append(tableName);
            if (!dbProductName.startsWith("ORACLE")
                    && !dbProductName.startsWith("DB2")
                    && !dbProductName.startsWith("MICROSOFT SQL SERVER")
                    && !dbProductName.startsWith("POSTGRESQL")) {
                // The AS clause is required for hsqldb. Some other databases might have
                // issues with it, so we're skipping some of them.
                query.append(" AS ").append(tableName);
            }
            query.append(" WHERE ");
            if (conditions != null && conditions.length() > 0) {
                // Put the user's conditions first.
                query.append("( ").append(conditions).append(" ) AND ");
            }

            // Now append the conditions associated with our split.
            query.append(conditionClauses.toString());

        } else {
            // User provided the query. We replace the special token with
            // our WHERE clause.
            String inputQuery = dbConf.getInputQuery();
            if (inputQuery.indexOf(com.cloudera.sqoop.mapreduce.db.DataDrivenDBInputFormat.SUBSTITUTE_TOKEN) == -1) {
                LOG.error("Could not find the clause substitution token "
                        + com.cloudera.sqoop.mapreduce.db.DataDrivenDBInputFormat.SUBSTITUTE_TOKEN + " in the query: ["
                        + inputQuery + "]. Parallel splits may not work correctly.");
            }

            query.append(inputQuery.replace(DataDrivenDBInputFormat.SUBSTITUTE_TOKEN,
                    conditionClauses.toString()));
        }

        LOG.debug("Using query: " + query.toString());

        return query.toString();
    }

    @Override
    public void close() throws IOException {
        try {
            if (null != currentResults) {
                currentResults.close();
            }
            // Statement.isClosed() is only available from JDBC 4
            // Some older drivers (like mysql 5.0.x and earlier fail with
            // the check for statement.isClosed()
            if (null != currentStatement) {
                currentStatement.close();
            }
            if (null != currentConnection && !currentConnection.isClosed()) {
                currentConnection.commit();
                currentConnection.close();
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    public void initialize(InputSplit inputSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {
        //do nothing
    }

    @Override
    public LongWritable getCurrentKey() {
        return key;
    }

    @Override
    public T getCurrentValue() {
        return value;
    }

    /**
     * @deprecated
     */
    @Deprecated
    public T createValue() {
        return ReflectionUtils.newInstance(inputClass, conf);
    }

    /**
     * @deprecated Use {@link #nextKeyValue()}
     */
    @Deprecated
    public boolean next(LongWritable k, T v) throws IOException {
        this.key = k;
        this.value = v;
        return nextKeyValue();
    }

    @Override
    public float getProgress() throws IOException {
        return (totalConnectionsSize - connections.size())* 1.0f/totalConnectionsSize;
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        try {
            if (key == null) {
                key = new LongWritable();
            }
            if (value == null) {
                value = createValue();
            }
            if (null == this.currentResults && currentConnection == null) {
                // First time into this method, run the query.
                currentConnection = switchConnection();
                if (null == currentConnection) {
                    return false;
                }
                this.currentResults = executeQuery(getSelectQuery(), currentConnection);
            }

            while (!this.currentResults.next()) {
                currentConnection = switchConnection();
                if (null == currentConnection) {
                    return false;
                }
                this.currentResults = executeQuery(getSelectQuery(), currentConnection);
            }
            // Set the key field value as the output key value
            key.set(pos + split.getStart());
            value.readFields(currentResults);
            pos++;
            return true;

        } catch (SQLException e) {
            LoggingUtils.logAll(LOG, e);
            close();
            throw new IOException("SQLException in nextKeyValue", e);
        }
    }

    private Connection switchConnection() throws IOException {
        close();
        String url = connections.poll();
        if (null == url)
            return null;
        else
            return getConnection(url);
    }

    private Connection getConnection(String url) {
        org.apache.sqoop.mapreduce.db.DBConfiguration dbConf = new org.apache.sqoop.mapreduce.db.DBConfiguration(conf);
        dbConf.getConf().set(org.apache.sqoop.mapreduce.db.DBConfiguration.URL_PROPERTY, url);
        Connection connection;
        try {
            connection = dbConf.getConnection();
            connection.setAutoCommit(false);
            DatabaseMetaData dbMeta = connection.getMetaData();
            String dbProductName = dbMeta.getDatabaseProductName().toUpperCase();
            if (conf
                    .getBoolean(com.cloudera.sqoop.mapreduce.db.DBConfiguration.PROP_RELAXED_ISOLATION, false)) {
                if (dbProductName.startsWith("ORACLE")) {
                    LOG.info("Using read committed transaction isolation for Oracle"
                            + " as read uncommitted is not supported");
                    connection.setTransactionIsolation(
                            Connection.TRANSACTION_READ_COMMITTED);
                } else {
                    LOG.info("Using read uncommited transaction isolation");
                    connection.setTransactionIsolation(
                            Connection.TRANSACTION_READ_UNCOMMITTED);
                }
            } else {
                LOG.info("Using read commited transaction isolation");
                connection.setTransactionIsolation(
                        Connection.TRANSACTION_READ_COMMITTED);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        LOG.info("Connection established!!! " + url);
        return connection;
    }

    /**
     * @return true if nextKeyValue() would return false.
     */
    protected boolean isDone() {
        try {
            return this.currentResults != null && currentResults.isAfterLast();
        } catch (SQLException sqlE) {
            return true;
        }
    }

    protected DBInputFormat.DBInputSplit getSplit() {
        return split;
    }

    protected String[] getFieldNames() {
        return fieldNames;
    }

    protected String getTableName() {
        return tableName;
    }

    protected String getConditions() {
        return conditions;
    }
}
