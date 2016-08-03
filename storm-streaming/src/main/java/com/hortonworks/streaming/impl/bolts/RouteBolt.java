package org.apache.storm.hbase.bolt;
//previous imported packages from TruckHBaseBolt
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Properties;

//new imported packages for TruckHBaseBolt
import org.apache.storm.utils.BatchHelper;
import org.apache.storm.utils.TupleUtils;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.hbase.security.HBaseSecurityUtil;
import org.apache.storm.hbase.common.ColumnList;

import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedList;

/**
 * Basic bolt for writing to HBase.
 *
 * Note: Each HBaseBolt defined in Topology is tied to a specific table.
 *
 */

public class RouteBolt  extends HBaseBolt {

    private static final Logger LOG = Logger.getLogger(TruckHBaseBolt.class);
    private static final byte[] INCIDENT_RUNNING_TOTAL_COLUMN = Bytes.toBytes("incidentRunningTotal");
    private static final long serialVersionUID = 2946379346389650318L;

    //three HBase table names with their associated column family names
    private static final String DANGEROUS_EVENTS_TABLE_NAME = "driver_dangerous_events";
    private static final String EVENTS_TABLE_COLUMN_FAMILY_NAME = "events";


    private static final String EVENTS_TABLE_NAME = "driver_events";
    private static final String ALL_EVENTS_TABLE_COLUMN_FAMILY_NAME = "allevents";

    private static final String EVENTS_COUNT_TABLE_NAME = "driver_dangerous_events_count";
    private static final String EVENTS_COUNT_TABLE_COLUMN_FAMILY_NAME = "counters";

    private OutputCollector collector;


    public void execute(Tuple input) {
        LOG.info("About to insert tuple[" + input + "] into HBase...");

        int driverId = input.getIntegerByField("driverId");
        int truckId = input.getIntegerByField("truckId");
        Timestamp eventTime = (Timestamp) input.getValueByField("eventTime");
        String eventType = input.getStringByField("eventType");
        double longitude = input.getDoubleByField("longitude");
        double latitude = input.getDoubleByField("latitude");
        String driverName = input.getStringByField("driverName");
        int routeId = input.getIntegerByField("routeId");
        String routeName = input.getStringByField("routeName");

        long incidentTotalCount = getInfractionCountForDriver(driverId);

        /*
        //Moved the hbase mapper to the topology
        if (!eventType.equals("Normal")) {
            try {
                //Store the incident event in HBase
                SimpleHBaseMapper mapper = new SimpleHBaseMapper()
                        .withRowKeyField("driverId" + "|" + "truckId" + "|" + "eventTime")
                        .withColumnFields(new Fields("driverId", "truckId", "eventTime", "eventType", "latitude", "longitude",
                                "driverName", "routeId", "routeName"))
                        .withColumnFamily(EVENTS_TABLE_COLUMN_FAMILY_NAME);

                LOG.info("Success inserting event into HBase table[" + DANGEROUS_EVENTS_TABLE_NAME + "]");
            } catch(Exception e){
                LOG.error("	Error inserting violation event into HBase table", e);
            }
        }
        */

        collector.emit(input, new Values(driverId, truckId, eventTime, eventType, longitude, latitude,
                incidentTotalCount, driverName, routeId, routeName));

        //acknowledge even if there is an error
        collector.ack(input);
    }

    private long getInfractionCountForDriver(int driverId) {
        try {
            byte[] driverCount = Bytes.toBytes(driverId);
            Get get = new Get(driverCount);
            Result result = eventsCountTable.get(get);
            long count = 0;
            if (result != null) {
                byte[] countBytes = result.getValue(Bytes.toBytes(EVENTS_COUNT_TABLE_COLUMN_FAMILY_NAME),
                        INCIDENT_RUNNING_TOTAL_COLUMN);
                if (countBytes != null) {
                    count = Bytes.toLong(countBytes);
                }

            }
            return count;
        } catch (Exception e) {
            LOG.error("Error getting infraction count", e);
            //return Long.MIN_VALUE;
            //throw new RuntimeException("Error getting infraction count");
        }
    }



}
