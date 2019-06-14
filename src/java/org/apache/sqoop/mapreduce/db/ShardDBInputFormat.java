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

import com.cloudera.sqoop.mapreduce.db.DBInputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.sqoop.config.ConfigurationHelper;
import org.apache.sqoop.mapreduce.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 分片导入InputFormat
 */
public class ShardDBInputFormat<T extends DBWritable>
        extends DBInputFormat<T> {

    private static final Log LOG = LogFactory.getLog(ShardDBInputFormat.class);

    /**
     * 数据库解析规则
     * 参考：readme
     */
   private static  final Pattern dbNum = Pattern.compile("(?<=\\[).+?(?=\\])");


    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        String allUrls = job.getConfiguration().get(DBConfiguration.ALL_URL_PROPERTY);
        int numMaps = ConfigurationHelper.getJobNumMaps(job);

        LOG.info("Input connection url is: " + allUrls);

        List<Queue<String>> mapperConnections = parseConnection(allUrls, numMaps);
        List<InputSplit> splits = new ArrayList<>();
        for (Queue<String> connections : mapperConnections) {
            LOG.info("This map was assigned "+ connections.size() + " shards");
            splits.add(new ShardDBInputSplit("1=1", "1=1", connections));
        }
        return splits;
    }





    private static List<String> match(List<String> connections,String input) {
        Matcher rangeMatcher = dbNum.matcher(input);
        if (rangeMatcher.find()) {
            String[] range = rangeMatcher.group().split("-");
            if (range.length == 2) {
                int start = Integer.parseInt(range[0].trim());
                int end = Integer.parseInt(range[1].trim());
                for (int i = start; i <= end; i++) {
                    String connection = input.substring(0, rangeMatcher.start() - 1) + i + input.substring(rangeMatcher.end() + 1);
                    if(dbNum.matcher(connection).find()){
                        match(connections,connection);
                    }else {
                        connections.add(connection);
                    }
                }
            }
        }
        return connections;
    }


    /**
     * 解析字符串连接
     * @param inputString
     * @param numMaps 每个字符串的并发导入数
     * @return
     */
    private List<Queue<String>> parseConnection(String inputString, int numMaps) {

        List<String> connections = new ArrayList<>();

        String[] inputCons = inputString.split(",");
        for(String input : inputCons){
            match(connections,input);
        }

        List<Queue<String>> mapperConnections = new ArrayList<>();

        int roundNumber = connections.size() / numMaps;
        int remainder = connections.size() % numMaps;

        Queue<String> connectionsPerMapper = new LinkedList<>();
        for (String connectionString : connections) {
            LOG.info("Import from: " + connectionString);
            connectionsPerMapper.add(connectionString);
            int perMapSize = roundNumber;
            if (remainder > 0)
                perMapSize = roundNumber + 1;
            if (connectionsPerMapper.size() == perMapSize) {
                mapperConnections.add(connectionsPerMapper);
                remainder--;
                connectionsPerMapper = new LinkedList<>();
            }
        }
        return mapperConnections;
    }

    @Override
    public Connection getConnection() {
        // in order to do nothing
        return null;
    }

    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {

        com.cloudera.sqoop.mapreduce.db.DBConfiguration dbConf = getDBConf();
        @SuppressWarnings("unchecked")
        Class<T> inputClass = (Class<T>) (dbConf.getInputClass());
        String dbProductName = getDBProductName();

        LOG.debug("Creating db record reader for db product: " + dbProductName);

        return new ShardDBRecordReader(split, inputClass,
                context.getConfiguration(), dbConf, dbConf.getInputConditions(),
                dbConf.getInputFieldNames(), dbConf.getInputTableName(),
                dbProductName);
    }

    public static void setInput(Job job,
                                Class<? extends DBWritable> inputClass,
                                String tableName, String conditions,
                                String splitBy, String... fieldNames) {
        DBInputFormat.setInput(job, inputClass, tableName, conditions,
                splitBy, fieldNames);
        job.setInputFormatClass(ShardDBInputFormat.class);
    }

    public static void setInput(Job job,
                                Class<? extends DBWritable> inputClass,
                                String inputQuery, String inputBoundingQuery) {
        DBInputFormat.setInput(job, inputClass, inputQuery, "");
        job.getConfiguration().set(com.cloudera.sqoop.mapreduce.db.DBConfiguration.INPUT_BOUNDING_QUERY,
                inputBoundingQuery);
        job.setInputFormatClass(ShardDBInputFormat.class);
    }

    public static class ShardDBInputSplit extends DataDrivenDBInputFormat.DataDrivenDBInputSplit {

        private Queue<String> connections;
        private int number;

        /**
         * Default Constructor.
         */
        public ShardDBInputSplit() {
            super();
        }

        ShardDBInputSplit(final String lower, final String upper, final Queue<String> connections) {
            super(lower, upper);
            this.connections = connections;
            this.number = connections.size();
        }

        public Queue<String> getConnections() {
            return connections;
        }

        @Override
        public void readFields(DataInput input) throws IOException {
            super.readFields(input);
            number = input.readInt();
            Queue<String> result = new LinkedList<>();
            for (int i = 0; i < number; i++) {
                result.add(Text.readString(input));
            }
            connections = result;
        }

        @Override
        public void write(DataOutput output) throws IOException {
            super.write(output);
            output.writeInt(number);
            for (String connection : connections) {
                Text.writeString(output, connection);
            }
        }
    }
}
