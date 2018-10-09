package com.zm.spark.constant;

/**
 * 常量接口
 * @author Luolidong
 *
 */
public interface Constants {

	/**
	 * KAFKA
	 */
	String KAFKA_METADATA_BROKER_LIST = "kafka.metadata.broker.list";


	/**
	 * SPARK
	 */
	String HBASE_ZK_PORT = "hbase.zookeeper.port";
	String HBASE_ZK_QUORUM = "hbase.zookeeper.quorum";

	/**
	 * REDIS
	 */
	String REDIS_PORT = "redis.port";
	String REDIS_HOST = "redis.host";
	String REDIS_PASSWORD = "redis.password";
	String REDIS_TIMEOUT = "redis.timeout";

	/**
	 * Test
	 */

	String TEST_POP_PROV_KAFKA_OFFSET_RESET = "test.pop.prov.kafka.offset.reset";
	String TEST_POP_PROV_KAFKA_TOPICS_SET = "test.pop.prov.kafka.topics.set";
	String TEST_POP_PROV_KAFKA_GROUP_ID = "test.pop.prov.kafka.group.id";
	String TEST_POP_PROV_SQL = "test.pop.prov.sql";
	String TEST_POP_PROV_HABSE_TABLE = "test.pop.prov.hbase.table";
	String TEST_POP_PROV_HBASE_TABLE_FAMILY = "test.pop.prov.hbase.table.family";
	String TEST_POP_PROV_HBASE_TABLE_QUALIFIER = "test.pop.prov.hbase.table.qualifier";
	String TEST_ONLINE_HABSE_TABLE = "test.online.hbase.table";




	/**
	 * Mysql
	 */
	String MYSQL_JDBC_DRIVER = "mysql.jdbc.driver";
	String MYSQL_USER = "mysql.user";
	String MYSQL_PASSWORD = "mysql.password";
	String MYSQL_URL = "mysql.url";
}
