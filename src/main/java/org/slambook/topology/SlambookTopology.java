package org.slambook.topology;

import java.util.UUID;

import org.slambook.topology.bolts.MaptoMongoDBBolt;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

public class SlambookTopology {
	private final String topology;
	private final Config topologyConfig;
	private final TopologyBuilder builder;
	
	public SlambookTopology(String topology) {
		this.topology = topology;
		builder = new TopologyBuilder();
		topologyConfig = new Config();
		topologyConfig.setDebug(true);
		wireTopology(topologyConfig);
	}

	private void wireTopology(Config topologyConfig2) {
		BrokerHosts hosts = new ZkHosts("localhost:2181");
		SpoutConfig spoutConfig = new SpoutConfig(hosts, "newuser", "/newuser", UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
		builder.setSpout("phraseTopic", kafkaSpout);
		MaptoMongoDBBolt bolt = new MaptoMongoDBBolt("localhost", 27017, "slambookusers", "slambook");
		builder.setBolt("mongoBolt", bolt).shuffleGrouping("phraseTopic");
	}

	public static void main(String[] args) throws InterruptedException {
		SlambookTopology topology = new SlambookTopology("umapranesh");
		topology.runLocally();
	}
	
	private void runLocally() throws InterruptedException{
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(topology, topologyConfig, builder.createTopology());
		Thread.sleep((long) 60 * 1000);
		cluster.killTopology(topology);
        cluster.shutdown();
	}
}
