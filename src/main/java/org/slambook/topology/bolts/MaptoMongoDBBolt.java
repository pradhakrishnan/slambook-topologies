package org.slambook.topology.bolts;

import java.net.UnknownHostException;
import java.util.Map;

import org.slambook.mongoservices.SlamBookServices;
import org.slambook.mongoservices.SlamBookUserServicesImpl;
import org.slambook.mongoservices.domain.SlamBookUser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class MaptoMongoDBBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String mongoHost;
	private int port;
	private String collectionName;
	private String database;
	private OutputCollector collector;
	private MongoClient client;
	private DB db;
	private ObjectMapper mapper;
	private SlamBookServices<SlamBookUser> slamBookServices;

	public MaptoMongoDBBolt(String mongoHost, int port, String collectionName,
			String db) {
		super();
		this.mongoHost = mongoHost;
		this.port = port;
		this.collectionName = collectionName;
		this.database = db;
	}

	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		try {
			slamBookServices = new SlamBookUserServicesImpl(mongoHost, port, database);
			client = new MongoClient(mongoHost, port);
			db = client.getDB(database);
			mapper = new ObjectMapper();
			slamBookServices.connectMongoServer();
		} catch (UnknownHostException e) {
			System.out.println("MongoDB connection error");
			throw new RuntimeException("MongoDB connection error");
		}

	}

	@SuppressWarnings("unchecked")
	public void execute(Tuple input) {
		try {
			System.out.println("**** Coming here *****"+input
					.getValueByField("str"));
		/*	Map<String, Object> values = mapper.readValue((String)input.getValueByField("str"), SlamBookUser.class);
			System.out.println("********* size*******"+values.size());
			DBObject dbObject = (DBObject) JSON.parse((String)input.getValueByField("str"));
			System.out.println("DB Object "+ dbObject.keySet().size());
			DBCollection collection = db.getCollection(collectionName);
			System.out.println("** collection*** "+collection.getName());
			collection.insert(dbObject);
			collector.ack(input);*/
			
			//using our service
			//SlamBookUser user = mapper.readValue((String)input.getValueByField("str"), SlamBookUser.class);
			Map<String, Object> values = mapper.readValue((String)input.getValueByField("str"), Map.class);
			SlamBookUser user = mapper.convertValue(values, SlamBookUser.class);
			System.out.println("********* email*******"+user.getEmail());
			slamBookServices.insertDocument(user, collectionName);
			collector.ack(input);
			
			
		} catch (Exception e) {
			System.err.println("Error while saving " + e.getMessage());
			collector.fail(input);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	@Override
	public void cleanup() {
		//this.db.getMongo().close();
		client.close();
		slamBookServices.connectMongoServer();
	}

}
