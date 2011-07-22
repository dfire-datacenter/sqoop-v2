/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.sqoop.manager;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.sqoop.SqoopOptions;
import org.apache.hadoop.sqoop.mapreduce.MySQLDumpImportJob;
import org.apache.hadoop.sqoop.util.ImportException;

/**
 * Manages direct connections to MySQL databases
 * so we can use mysqldump to get really fast dumps.
 */
public class DirectMySQLManager extends MySQLManager {

  public static final Log LOG = LogFactory.getLog(DirectMySQLManager.class.getName());

  public DirectMySQLManager(final SqoopOptions options) {
    super(options, false);
  }

  /**
   * Import the table into HDFS by using mysqldump to pull out the data from
   * the database and upload the files directly to HDFS.
   */
  public void importTable(ImportJobContext context)
      throws IOException, ImportException {

    if (context.getOptions().getColumns() != null) {
      LOG.warn("Direct-mode import from MySQL does not support column");
      LOG.warn("selection. Falling back to JDBC-based import.");
      // Don't warn them "This could go faster..."
      MySQLManager.warningPrinted = true;
      // Use JDBC-based importTable() method.
      super.importTable(context);
      return;
    }

    String tableName = context.getTableName();
    String jarFile = context.getJarFile();
    SqoopOptions options = context.getOptions();

    MySQLDumpImportJob importer = new MySQLDumpImportJob(options);

    String splitCol = getSplitColumn(options, tableName);
    if (null == splitCol && options.getNumMappers() > 1) {
      // Can't infer a primary key.
      throw new ImportException("No primary key could be found for table "
          + tableName + ". Please specify one with --split-by or perform "
          + "a sequential import with '-m 1'.");
    }

    LOG.info("Beginning mysqldump fast path import");

    if (options.getFileLayout() != SqoopOptions.FileLayout.TextFile) {
      // TODO(aaron): Support SequenceFile-based load-in.
      LOG.warn("File import layout " + options.getFileLayout()
          + " is not supported by");
      LOG.warn("MySQL direct import; import will proceed as text files.");
    }

    importer.runImport(tableName, jarFile, splitCol, options.getConf());
  }
}
