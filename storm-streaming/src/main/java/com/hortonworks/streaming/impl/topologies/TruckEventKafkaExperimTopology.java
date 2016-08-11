//Previous import packages from TruckEventKafkatopology
package com.hortonworks.streaming.impl.topologies;

import com.hortonworks.streaming.impl.bolts.common.EventTypeStream;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import com.hortonworks.streaming.impl.bolts.*;
import com.hortonworks.streaming.impl.bolts.hdfs.FileTimeRotationPolicy;
import com.hortonworks.streaming.impl.bolts.hive.HiveTablePartitionAction;
import com.hortonworks.streaming.impl.kafka.TruckScheme2;
import org.apache.log4j.Logger;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;

import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.hbase.security.HBaseSecurityUtil;

import java.util.HashMap;
import java.util.Map;

public class TruckEventKafkaExperimTopology extends BaseTruckEventTopology {
    private static final Logger LOG = Logger.getLogger(TruckEventKafkaExperimTopology.class);

    private static final String DANGEROUS_EVENTS_TABLE_NAME = "driver_dangerous_events";
    private static final String EVENTS_TABLE_COLUMN_FAMILY_NAME = "events";

    private static final String EVENTS_TABLE_NAME = "driver_events";
    private static final String ALL_EVENTS_TABLE_COLUMN_FAMILY_NAME = "allevents";

    private static final String EVENTS_COUNT_TABLE_NAME = "driver_dangerous_events_count";
    private static final String EVENTS_COUNT_TABLE_COLUMN_FAMILY_NAME = "counters";

    public TruckEventKafkaExperimTopology(String configFileLocation) throws Exception {
        super(configFileLocation);
    }

    public static void main(String[] args) throws Exception {
        String configFileLocation = args[0];

        // kafkaspout ==> RouteBolt-writes to one hbase table
        TruckEventKafkaExperimTopology truckTopology = new TruckEventKafkaExperimTopology(configFileLocation);
        truckTopology.buildAndSubmit();
    }

    public void buildAndSubmit() throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

	/* This config is for Storm and it needs be configured with things like the following:
         * 	Zookeeper server, nimbus server, ports, etc... All of this configuration will be picked up
		 * in the ~/.storm/storm.yaml file that will be located on each storm node.
		 */
        Config config = new Config();
        config.setDebug(true);

        Map<String, Object> hbaseConf = new HashMap<String, Object>();
        config.put("hbase.conf", hbaseConf);
                
        /* Set the number of workers that will be spun up for this topology.
         * Each worker represents a JVM where executor thread will be spawned from */
        Integer topologyWorkers = Integer.valueOf(topologyConfig.getProperty("storm.trucker.topology.workers"));
        config.put(Config.TOPOLOGY_WORKERS, topologyWorkers);

        //Read the nimbus host in from the config file as well
        String nimbusHost = topologyConfig.getProperty("nimbus.host");
        config.put(Config.NIMBUS_HOST, nimbusHost);

        // Set up Kafka Spout to ingest from
        configureKafkaSpout(builder);


        configureRouteBolt(builder);

        /* Set up HBaseBolt to write to HBase tables */
        configureHBaseBolt(builder);

        //Try to submit topology
        try {
            StormSubmitter.submitTopology("truck-event-processor", config, builder.createTopology());
        } catch (Exception e) {
            LOG.error("Error submiting Topology", e);
        }

    }

    /* Set up Kafka Spout to ingest data from simulator */
    public int configureKafkaSpout(TopologyBuilder builder) {
        KafkaSpout kafkaSpout = constructKafkaSpout();

        int spoutCount = Integer.valueOf(topologyConfig.getProperty("spout.thread.count"));
        int boltCount = Integer.valueOf(topologyConfig.getProperty("bolt.thread.count"));

        builder.setSpout("kafkaSpout", kafkaSpout, spoutCount);
        return boltCount;
    }


    private KafkaSpout constructKafkaSpout() {
        KafkaSpout kafkaSpout = new KafkaSpout(constructKafkaSpoutConf());
        return kafkaSpout;
    }

    private SpoutConfig constructKafkaSpoutConf() {
        BrokerHosts hosts = new ZkHosts(topologyConfig.getProperty("kafka.zookeeper.host.port"));
        String topic = topologyConfig.getProperty("kafka.topic");
        String zkRoot = topologyConfig.getProperty("kafka.zkRoot");
        String consumerGroupId = topologyConfig.getProperty("kafka.consumer.group.id");

        SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, consumerGroupId);

		/* Custom TruckScheme that will take Kafka message of single truckEvent
		 * and emit a 2-tuple consisting of truckId and truckEvent. This driverId
		 * is required to do a fieldsSorting so that all driver events are sent to the set of bolts */
        spoutConfig.scheme = new SchemeAsMultiScheme(new TruckScheme2());

        return spoutConfig;
    }

    public void configureRouteBolt(TopologyBuilder builder) {
        RouteBolt routeBolt = new RouteBolt(true);
        //Defines new bolt in topology
        builder.setBolt("routeBolt", routeBolt, 2).shuffleGrouping("kafkaSpout");
    }

    public void configureHBaseBolt(TopologyBuilder builder) {
        try {
            setAllEventsBolt(builder);

            setNotNormalEventsBolt(builder);

        } catch (Exception e) {
            LOG.error("	Error inserting violation event into HBase table", e);
        }
    }

    private void setAllEventsBolt(TopologyBuilder builder) {
        //Store the all events in HBase DriverEventsTable
        HBaseBolt hbaseDriverEventsTable = new HBaseBolt(EVENTS_TABLE_NAME, getMapperDriverEventsTable()).withConfigKey("hbase.conf");

        builder.setBolt("hbaseAllDriverEvents", hbaseDriverEventsTable, 2).fieldsGrouping("routeBolt", getFields());
    }

    private SimpleHBaseMapper getMapperDriverEventsTable() {
        return new SimpleHBaseMapper()
                .withRowKeyField("hbaseRowKey")
                .withColumnFields(getFields())
                .withColumnFamily(ALL_EVENTS_TABLE_COLUMN_FAMILY_NAME);
    }

    private void setNotNormalEventsBolt(TopologyBuilder builder) {
        System.out.println("Stream ID: " + EventTypeStream.NOT_NORMAL.toString());
        //Store incident events into HBase Table driver_dangerous_events
        final HBaseBolt hbaseDriverDangerousEventsTable =
                new HBaseBolt(DANGEROUS_EVENTS_TABLE_NAME, getMapperDangerousEventsTable()).withConfigKey("hbase.conf");

        builder.setBolt("hbaseDangerousEvents", hbaseDriverDangerousEventsTable, 2)
                .fieldsGrouping("routeBolt", EventTypeStream.NOT_NORMAL.toString(), getFields());

        //Update the running count of all incidents for driver_dangerous_events_count HBase Table
        final HBaseBolt hbaseEventsCountTable = new HBaseBolt(EVENTS_COUNT_TABLE_NAME, getMapperEventsCountTable()).withConfigKey("hbase.conf");
        builder.setBolt("hbaseDangerousEventsCount", hbaseEventsCountTable, 2)
                .fieldsGrouping("routeBolt", EventTypeStream.NOT_NORMAL.toString(),
                        new Fields("driverId", "incidentRunningTotal"));
    }

    private SimpleHBaseMapper getMapperEventsCountTable() {
        return new SimpleHBaseMapper()
                .withRowKeyField("driverId")
                .withCounterFields(new Fields("incidentRunningTotal"))
                .withColumnFamily(EVENTS_COUNT_TABLE_COLUMN_FAMILY_NAME);
    }

    private SimpleHBaseMapper getMapperDangerousEventsTable() {
        return new SimpleHBaseMapper()
                .withRowKeyField("hbaseRowKey")
                .withColumnFields(getFields())
                .withColumnFamily(EVENTS_TABLE_COLUMN_FAMILY_NAME);
    }

    // TODO: Create public static fields for strings

    // TODO: Schould probably be in a global place to be called by all the bolts
    private Fields getFields() {
        return new Fields("driverId", "truckId", "eventTime", "eventType", "latitude", "longitude", "driverName", "routeId", "routeName", "hbaseRowKey");
    }
}
