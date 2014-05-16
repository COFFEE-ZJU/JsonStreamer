package cn.edu.zju.jsonStreamer.parse;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.thrift7.TException;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.daemon.supervisor;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.KillOptions;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.NotAliveException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import cn.edu.zju.jsonStreamer.IO.output.JStreamOutput;
import cn.edu.zju.jsonStreamer.constants.Constants;
import cn.edu.zju.jsonStreamer.constants.Constants.JsonOpType;
import cn.edu.zju.jsonStreamer.constants.Constants.StormFields;
import cn.edu.zju.jsonStreamer.constants.JsonStreamerException;
import cn.edu.zju.jsonStreamer.constants.SemanticErrorException;
import cn.edu.zju.jsonStreamer.constants.SyntaxErrorException;
import cn.edu.zju.jsonStreamer.constants.SystemErrorException;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonError;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonQueryTree;
import cn.edu.zju.jsonStreamer.operators.Operator;
import cn.edu.zju.jsonStreamer.operators.storm.AggregationOperator;
import cn.edu.zju.jsonStreamer.operators.storm.DStreamOperator;
import cn.edu.zju.jsonStreamer.operators.storm.ExpandOperator;
import cn.edu.zju.jsonStreamer.operators.storm.IStreamOperator;
import cn.edu.zju.jsonStreamer.operators.storm.JoinOperator;
import cn.edu.zju.jsonStreamer.operators.storm.LeafOperator;
import cn.edu.zju.jsonStreamer.operators.storm.ProjectionOperator;
import cn.edu.zju.jsonStreamer.operators.storm.RStreamOperator;
import cn.edu.zju.jsonStreamer.operators.storm.RangeWindowOperator;
import cn.edu.zju.jsonStreamer.operators.storm.RootOperator;
import cn.edu.zju.jsonStreamer.operators.storm.RowWindowOperator;
import cn.edu.zju.jsonStreamer.operators.storm.SelectionOperator;
import cn.edu.zju.jsonStreamer.operators.storm.StormGrouper;
import cn.edu.zju.jsonStreamer.operators.storm.StreamReceiver;
import cn.edu.zju.jsonStreamer.operators.storm.StreamSender;
import cn.edu.zju.jsonStreamer.scheduler.Scheduler;
import cn.edu.zju.jsonStreamer.utils.ElementIdGenerator;

import com.google.gson.reflect.TypeToken;

//TODO
public class RegisterForStorm extends Register{
	private JStreamOutput outStream;
	private List<Operator> opList = null;
	private List<Operator> tmpOps = null;
	private long topologyId;
	private String ip;
	private Queue<String> topologies = null;
	private Map<JsonQueryTree, String> nodeMap = null;
	private LocalCluster localCluster = null;		//for local mode
	private TopologyBuilder builder;
	
	public RegisterForStorm(){
		opList = new LinkedList<Operator>();
		topologies = new LinkedList<String>();
		nodeMap = new HashMap<JsonQueryTree, String>();
		if(Constants.STORM_LOCAL) localCluster = new LocalCluster();
	}
	
	@Override
	public void cancelQuery() throws SystemErrorException{
		KillOptions killOpts = new KillOptions();
		killOpts.set_wait_secs(2);	//TODO
		if(Constants.STORM_LOCAL){
			while(!topologies.isEmpty()) localCluster.killTopologyWithOpts(topologies.poll(), killOpts);
		}
		else{
			Map conf = Utils.readStormConfig();
			Client client = NimbusClient.getConfiguredClient(conf).getClient();
			while(!topologies.isEmpty()){
				try {
					client.killTopologyWithOpts(topologies.poll(), killOpts);
				} catch (NotAliveException | TException e) {
					throw new SystemErrorException(e);
				}
			}
		}
		
		Scheduler.getInstance().removeOperators(opList);
		opList = null;
	}
	
	@Override
	public void register(String jsonApiString, JStreamOutput outStream) throws JsonStreamerException{
		super.register(jsonApiString, outStream);
//		if(opList != null || topologies != null) throw new SystemErrorException("double parse for one parser instance");
		
		List<JsonQueryTree> jqt = Constants.gson.fromJson(jsonApiString, new TypeToken<List<JsonQueryTree> >(){}.getType());
		this.outStream = outStream;
		ip = Constants.getMyIp();
		topologyId = ElementIdGenerator.getNewId();
		String topName = "JsonStreamerTopology"+topologyId;
		tmpOps = new LinkedList<Operator>();
		builder = new TopologyBuilder();
		Iterator<JsonQueryTree> it = jqt.iterator();
		JsonQueryTree curTree;
		while(it.hasNext()){
			curTree = it.next();
			parse(curTree);
		}
		Config conf = new Config();
		conf.setDebug(true);
		if(Constants.STORM_LOCAL){
			localCluster.submitTopology(topName, conf, builder.createTopology());
			topologies.add(topName);
		}
		else{
			try {
				conf.setNumWorkers(3);
				StormSubmitter.submitTopology(topName, conf, builder.createTopology());
				topologies.add(topName);
			} catch (AlreadyAliveException | InvalidTopologyException e) {
				throw new SystemErrorException(e);
			}
		}
		
		Scheduler.getInstance().addOperators(tmpOps);
		opList.addAll(tmpOps);
	}
	
	private String parse(JsonQueryTree tree) throws JsonStreamerException{	//returns operator's className + unique id
//		Operator retOp = null, subOp = null, subOp2 = null;
		String opName = null, subOpName = null;
		if(tree.type != JsonOpType.ROOT && tree.type != JsonOpType.ERROR){
			opName = nodeMap.get(tree);
			if(opName != null) return opName;
		}
		
		switch (tree.type) {
		case ROOT:
			StreamReceiver sr = new StreamReceiver(tree, outStream, ElementIdGenerator.getNewId());
			RootOperator ro = new RootOperator(ip, sr.getQueueName());
			subOpName = parse(tree.input);
			opName = ro.getClass().getSimpleName()+ElementIdGenerator.getNewId();
			builder.setBolt(opName, ro, 1).shuffleGrouping(subOpName);
			tmpOps.add(sr);
			break;
			
		case LEAF:
			StreamSender ss = new StreamSender(tree, ElementIdGenerator.getNewId());
			LeafOperator lo = new LeafOperator(tree, ip, ss.getQueueName());
			opName = lo.getClass().getSimpleName()+ElementIdGenerator.getNewId();
			builder.setSpout(opName, lo, 1);
			tmpOps.add(ss);
			break;
			
		case ROWWINDOW:
			RowWindowOperator row = new RowWindowOperator(tree);
			opName = row.getClass().getSimpleName()+ElementIdGenerator.getNewId();
			subOpName = parse(tree.input);
			builder.setBolt(opName, row, 1).shuffleGrouping(subOpName);
			break;
			
		case RANGEWINDOW:
			RangeWindowOperator range = new RangeWindowOperator(tree);
			opName = range.getClass().getSimpleName()+ElementIdGenerator.getNewId();
			subOpName = parse(tree.input);
			builder.setBolt(opName, range, 1).shuffleGrouping(subOpName);
			break;
			
		case DSTREAM:
			DStreamOperator dso = new DStreamOperator(tree);
			opName = dso.getClass().getSimpleName()+ElementIdGenerator.getNewId();
			subOpName = parse(tree.input);
			builder.setBolt(opName, dso, 1).shuffleGrouping(subOpName);
			break;

		case RSTREAM:
			RStreamOperator rso = new RStreamOperator(tree);
			opName = rso.getClass().getSimpleName()+ElementIdGenerator.getNewId();
			subOpName = parse(tree.input);
			builder.setBolt(opName, rso, 1).shuffleGrouping(subOpName);
			break;
			
		case ISTREAM:
			IStreamOperator iso = new IStreamOperator(tree);
			opName = iso.getClass().getSimpleName()+ElementIdGenerator.getNewId();
			subOpName = parse(tree.input);
			builder.setBolt(opName, iso, 1).shuffleGrouping(subOpName);
			break;
			
		case SELECTION:
			SelectionOperator so = new SelectionOperator(tree);
			opName = so.getClass().getSimpleName()+ElementIdGenerator.getNewId();
			subOpName = parse(tree.input);
			builder.setBolt(opName, so, Constants.PARALLELISM).fieldsGrouping(subOpName, new Fields(StormFields.id));
			break;
			
		case GROUPBY_AGGREGATION:
			StormGrouper sg = new StormGrouper(tree.groupby_attribute_name);
			String sgName = sg.getClass().getSimpleName()+ElementIdGenerator.getNewId();
			AggregationOperator ao = new AggregationOperator(tree);
			opName = ao.getClass().getSimpleName()+ElementIdGenerator.getNewId();
			subOpName = parse(tree.input);
			builder.setBolt(sgName, sg, Constants.PARALLELISM).fieldsGrouping(subOpName, new Fields(StormFields.id));
			builder.setBolt(opName, ao, Constants.PARALLELISM).fieldsGrouping(sgName, new Fields(StormFields.groupKey));
			break;
			
		case EXPAND:
			ExpandOperator eo = new ExpandOperator(tree);
			opName = eo.getClass().getSimpleName()+ElementIdGenerator.getNewId();
			subOpName = parse(tree.input);
			builder.setBolt(opName, eo, Constants.PARALLELISM).fieldsGrouping(subOpName, new Fields(StormFields.id));
			break;

		case PROJECTION:
			ProjectionOperator po = new ProjectionOperator(tree);
			opName = po.getClass().getSimpleName()+ElementIdGenerator.getNewId();
			subOpName = parse(tree.input);
			builder.setBolt(opName, po, Constants.PARALLELISM).fieldsGrouping(subOpName, new Fields(StormFields.id));
			break;
			
		case JOIN:
			StormGrouper sg1 = new StormGrouper(tree.left_join_attribute, 1);
			String sg1Name = sg1.getClass().getSimpleName()+ElementIdGenerator.getNewId();
			StormGrouper sg2 = new StormGrouper(tree.right_join_attribute, 2);
			String sg2Name = sg2.getClass().getSimpleName()+ElementIdGenerator.getNewId();
			
			JoinOperator jo = new JoinOperator(tree);
			opName = jo.getClass().getSimpleName()+ElementIdGenerator.getNewId();
			subOpName = parse(tree.left_input);
			String subOpName2 = parse(tree.right_input);
			builder.setBolt(sg1Name, sg1, Constants.PARALLELISM).fieldsGrouping(subOpName, new Fields(StormFields.id));
			builder.setBolt(sg2Name, sg2, Constants.PARALLELISM).fieldsGrouping(subOpName2, new Fields(StormFields.id));
			
			builder.setBolt(opName, jo, Constants.PARALLELISM)
				.fieldsGrouping(sg1Name, new Fields(StormFields.groupKey))
				.fieldsGrouping(sg2Name, new Fields(StormFields.groupKey));
			break;
			
		case PARTITIONWINDOW:
			throw new SemanticErrorException("partition window not supported yet");
			
		case ERROR:
			JsonError error = tree.error_info;
			switch (error.error_type) {
			case SEMANTIC_ERROR:
				throw new SemanticErrorException(error.error_message);
			case SYNTAX_ERROR:
				throw new SyntaxErrorException(error.error_message);
			}
			throw new SystemErrorException("shouldn't be here");
			
		default:
			throw new SystemErrorException("shouldn't be here");
		}
		
		nodeMap.put(tree, opName);
		return opName;
	}

}
