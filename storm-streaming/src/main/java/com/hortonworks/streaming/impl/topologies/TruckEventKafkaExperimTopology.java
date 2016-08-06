//Previous import packages from TruckEventKafkatopology
package com.hortonworks.streaming.impl.topologies;
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

        // Set up Kafka Spout to ingest from Simulator
        configureKafkaSpout(builder);

	// Set up RouteBolt to emit tuples to be collected by other bolts
        configureRouteBolt(builder);

        // Set up HBaseBolt to collect tuples from RouteBolt and write to HBase tables
        configureHBaseBolt(builder);
        
        //Try to submit topology
        try {
            StormSubmitter.submitTopology("truck-event-processor", config, builder.createTopology());
        } catch (Exception e) {
            LOG.error("Error submiting Topology", e);
        }
        
    }



    // Set up Kafka Spout to ingest data from simulator
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
    
    //Set up RouteBolt to collect tuples from kafkaspout and emit tuples to other entities
    public void configureRouteBolt(TopologyBuilder builder) {
        RouteBolt hbase = new RouteBolt(true);
        //Defines new bolt in topology
        builder.setBolt("route_bolt", hbase, 2).shuffleGrouping("kafkaSpout");

    }

    //Set up HBaseBolt to collect tuples from RouteBolt and write data to HBase Tables
    public void configureHBaseBolt(TopologyBuilder builder){


        //Store the incident event in HBase Table driver_dangerous_events
        SimpleHBaseMapper mapper_DangerousEventsTable = new SimpleHBaseMapper()
                .withRowKeyField("hbaseRowKey")
                .withColumnFields(new Fields("driverId", "truckId", "eventTime", "eventType", "latitude", "longitude",
                        "driverName", "routeId", "routeName"))
                .withColumnFamily(EVENTS_TABLE_COLUMN_FAMILY_NAME);

        HBaseBolt hbase = new HBaseBolt(DANGEROUS_EVENTS_TABLE_NAME, mapper_DangerousEventsTable).withConfigKey("hbase.conf");
        builder.setBolt("Hbase_Bolt", hbase, 2).fieldsGrouping("route_bolt", new Fields("driverId", "truckId",
                "eventTime", "eventType", "latitude", "longitude", "driverName", "routeId", "routeName", "hbaseRowKey"));



        //Update the running count of all incidents for driver_dangerous_events_count HBase Table
      /*  SimpleHBaseMapper mapper_EventsCountTable = new SimpleHBaseMapper()
                .withRowKeyField("driverId")
                .withCounterFields(new Fields("incidentRunningTotal"))
                .withColumnFamily(EVENTS_COUNT_TABLE_COLUMN_FAMILY_NAME);

        HBaseBolt hbase = new HBaseBolt(EVENTS_COUNT_TABLE_NAME, mapper_EventsCountTable).withConfigKey("hbase.conf");

        //Write to HBase DriverEventsTable
        SimpleHBaseMapper mapper_DriverEventsTable = new SimpleHBaseMapper()
                 .withRowKeyField(hbaseRowKey)
                 .withColumnFields(new Fields("driverId", "truckId", "eventTime", "eventType", "latitude", "longitude",
                                "driverName", "routeId", "routeName"))
                 .withColumnFamily(ALL_EVENTS_TABLE_COLUMN_FAMILY_NAME);

        HBaseBolt hbase = new HBaseBolt(EVENTS_TABLE_NAME, mapper_DriverEventsTable).withConfigKey("hbase.conf");

        */


    }
}
