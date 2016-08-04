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

//New import packages for topology
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.hbase.security.HBaseSecurityUtil;
import java.util.HashMap;
import java.util.Map;

public class TruckEventKafkaExperimTopology extends BaseTruckEventTopology {
    private static final Logger LOG = Logger.getLogger(TruckEventKafkaExperimTopology.class);
    
    private static final String DANGEROUS_EVENTS_TABLE_NAME = "driver_dangerous_events";
    private static final String EVENTS_TABLE_COLUMN_FAMILY_NAME = "events";

    static private String[] theArgs;

    public TruckEventKafkaExperimTopology(String configFileLocation) throws Exception {
        super(configFileLocation);
    }

    public static void main(String[] args) throws Exception {
        String configFileLocation = args[0];
        theArgs = args;
        
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
        
        
                
        /* Set the number of workers that will be spun up for this topology.
		 * Each worker represents a JVM where executor thread will be spawned from */
        Integer topologyWorkers = Integer.valueOf(topologyConfig.getProperty("storm.trucker.topology.workers"));
        config.put(Config.TOPOLOGY_WORKERS, topologyWorkers);

        //Read the nimbus host in from the config file as well
        String nimbusHost = topologyConfig.getProperty("nimbus.host");
        config.put(Config.NIMBUS_SEED, nimbusHost);

        /* Set up Kafka Spout to ingest from */
        configureKafkaSpout(builder);
        
        /* Set up HBaseBolt to write to HBase tables */
        configureHBaseBolt(builder, config);
        
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
    
    public int configureHBaseBolt(TopologyBuilder builder, Config config){
    	/* Setup HBase Bolt to persist violations and all events (if configured to do so)*/
        Map<String, Object> hbConf = new HashMap<String, Object>();
        if(theArgs.length > 0){
            hbConf.put("hbase.rootdir", theA:rgs[0]);
        }
        config.put("hbase.conf", hbConf);

	//try {
                //Store the incident event in HBase Table driver_dangerous_events
                SimpleHBaseMapper mapper = new SimpleHBaseMapper()
                        .withRowKeyField("driverId" + "|" + "truckId" + "|" + "eventTime")
                        .withColumnFields(new Fields("driverId", "truckId", "eventTime", "eventType", "latitude", "longitude", 
                                "driverName", "routeId", "routeName"))
                        .withColumnFamily(EVENTS_TABLE_COLUMN_FAMILY_NAME);

                LOG.info("Success inserting event into HBase table[" + DANGEROUS_EVENTS_TABLE_NAME + "]");
        /*} catch(Exception e){
                LOG.error("	Error inserting violation event into HBase table", e);
        }*/

        
        HBaseBolt hbase = new HBaseBolt(DANGEROUS_EVENTS_TABLE_NAME, mapper).withConfigKey("hbase.conf");
        builder.setBolt("hbase_bolt", hbase, 2).fieldsGrouping("kafkaSpout", new Fields("driverId", "truckId",
                "eventTime", "eventType", "latitude", "longitude", "driverName", "routeId", "routeName"));
    }
}
