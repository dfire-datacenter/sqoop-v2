
= Welcome to Sqoop!

This is the Sqoop (SQL-to-Hadoop) tool. Sqoop allows easy imports and
exports of data sets between databases and HDFS.


== More Documentation

Sqoop ships with additional documentation: a user guide and a manual page.

Asciidoc sources for both of these are in +src/docs/+. Run +ant docs+ to build
the documentation. It will be created in +build/docs/+.

If you got Sqoop in release form, documentation will already be built and
available in the +docs/+ directory.


== Compiling Sqoop

Compiling Sqoop requires the following tools:

* Apache ant (1.7.1)
* Java JDK 1.6

Additionally, building the documentation requires these tools:

* asciidoc
* make
* python 2.5+
* xmlto
* tar
* gzip

To compile Sqoop, run +ant package+. There will be a fully self-hosted build
provided in the +build/sqoop-(version)/+ directory. 

You can build just the jar by running +ant jar+.

See the COMPILING.txt document for for information.

== This is also an Asciidoc file!

优化东西：
1.分库导出减少code 生成时间，128个库，只生成1次
2.支持分库分表导出
3.减少客户端提交导出导致driver多，启动时间长

总得来说：128库 300W 数据，5分钟左右能导出


# 其他说明，支持多个数据源导出，比如分库分表：
sqoop import --connect  ${datasource}  \
  --include-databases ${partitions_databases} \
  --paraller-num ${paraller_num} \
  --table ${table} \
  --username ${partitions_username} \
  --password ${partitions_password}  \
  --columns ${columns}   \
  --split-by ${split_by}   \
  --delete-target-dir \
  --hive-database ${hive_database_org}  \
  --hive-import \
  --hive-overwrite  \
  --hive-table ${table}  \
  -m ${maps}  \
  --input-fields-terminated-by '\001'  \
  --hive-drop-import-delims \
  --hive-partition-key  pt \
  --hive-partition-value 0  \
  --null-string '\\N' \
  --null-non-string '\\N'
  if [ $? -ne 0 ] ; then
    echo "ERROR : import ${hive_database}-${table} failed"
    exit 255
  fi


  connect:这个参数不能省略，兼容用的，导致比较麻烦。默认导出用第一个库就行了
  比如：128个库会导致，1+127 的写法
  eg: jdbc:mysql://xxx:3306/table1  1:表示第一个库

  include-databases:数据库地址集合

  eg：jdbc:mysql://xxx:3306/table[2-63],jdbc:mysql://xxx:3306/table[64-128]

  paraller-num ：并发，这个参数会导致 -m 参数无效

* Try running +asciidoc README.txt+
* For more information about asciidoc, see http://www.methods.co.nz/asciidoc/

