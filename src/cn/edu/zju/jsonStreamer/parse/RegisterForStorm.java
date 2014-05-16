package cn.edu.zju.jsonStreamer.parse;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.thrift7.TException;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
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
	private long topologyId;
	private String ip;
	private Queue<String> topologies = null;
	private LocalCluster localCluster = null;		//for local mode
	private TopologyBuilder builder;
	
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
	public void parse(String jsonApiString, JStreamOutput outStream) throws JsonStreamerException{
		if(opList != null || topologies != null) throw new SystemErrorException("double parse for one parser instance");
		
		List<JsonQueryTree> jqt = Constants.gson.fromJson(jsonApiString, new TypeToken<List<JsonQueryTree> >(){}.getType());
		this.outStream = outStream;
		topologyId = ElementIdGenerator.getNewId();
		ip = Constants.getMyIp();
		opList = new LinkedList<Operator>();
		topologies = new LinkedList<String>();
		if(Constants.STORM_LOCAL) localCluster = new LocalCluster();
		
		Iterator<JsonQueryTree> it = jqt.iterator();
		JsonQueryTree curTree;
		while(it.hasNext()){
			builder = new TopologyBuilder();
			curTree = it.next();
			String topName = parse(curTree);
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
		}
		Scheduler.getInstance().addOperators(opList);

	}
	
	private String parse(JsonQueryTree tree) throws JsonStreamerException{	//returns operator's className + topologyId
//		Operator retOp = null, subOp = null, subOp2 = null;
		String opName, subOpName;
		switch (tree.type) {
		case ROOT:
			StreamReceiver sr = new StreamReceiver(tree, outStream, topologyId);
			RootOperator ro = new RootOperator(ip, sr.getQueueName());
			subOpName = parse(tree.input);
			opName = ro.getClass().getSimpleName()+topologyId;
			builder.setBolt(opName, ro, 1).shuffleGrouping(subOpName);
			opList.add(sr);
			return opName;
			
		case LEAF:
			StreamSender ss = new StreamSender(tree, topologyId);
			LeafOperator lo = new LeafOperator(tree, ip, ss.getQueueName());
			opName = lo.getClass().getSimpleName()+topologyId;
			builder.setSpout(opName, lo, 1);
			opList.add(ss);
			return opName;
			
		case ROWWINDOW:
			RowWindowOperator row = new RowWindowOperator(tree);
			opName = row.getClass().getSimpleName()+topologyId;
			subOpName = parse(tree.input);
			builder.setBolt(opName, row, 1).shuffleGrouping(subOpName);
			return opName;
			
		case RANGEWINDOW:
			RangeWindowOperator range = new RangeWindowOperator(tree);
			opName = range.getClass().getSimpleName()+topologyId;
			subOpName = parse(tree.input);
			builder.setBolt(opName, range, 1).shuffleGrouping(subOpName);
			return opName;
			
		case DSTREAM:
			DStreamOperator dso = new DStreamOperator(tree);
			opName = dso.getClass().getSimpleName()+topologyId;
			subOpName = parse(tree.input);
			builder.setBolt(opName, dso, 1).shuffleGrouping(subOpName);
			return opName;

		case RSTREAM:
			RStreamOperator rso = new RStreamOperator(tree);
			opName = rso.getClass().getSimpleName()+topologyId;
			subOpName = parse(tree.input);
			builder.setBolt(opName, rso, 1).shuffleGrouping(subOpName);
			return opName;
			
		case ISTREAM:
			IStreamOperator iso = new IStreamOperator(tree);
			opName = iso.getClass().getSimpleName()+topologyId;
			subOpName = parse(tree.input);
			builder.setBolt(opName, iso, 1).shuffleGrouping(subOpName);
			return opName;
			
		case SELECTION:
			SelectionOperator so = new SelectionOperator(tree);
			opName = so.getClass().getSimpleName()+topologyId;
			subOpName = parse(tree.input);
			builder.setBolt(opName, so, Constants.PARALLELISM).fieldsGrouping(subOpName, new Fields(StormFields.id));
			return opName;
			
		case GROUPBY_AGGREGATION:
			StormGrouper sg = new StormGrouper(tree.groupby_attribute_name);
			String sgName = sg.getClass().getSimpleName()+topologyId;
			AggregationOperator ao = new AggregationOperator(tree);
			opName = ao.getClass().getSimpleName()+topologyId;
			subOpName = parse(tree.input);
			builder.setBolt(sgName, sg, Constants.PARALLELISM).fieldsGrouping(subOpName, new Fields(StormFields.id));
			builder.setBolt(opName, ao, Constants.PARALLELISM).fieldsGrouping(sgName, new Fields(StormFields.groupKey));
			return opName;
			
		case EXPAND:
			ExpandOperator eo = new ExpandOperator(tree);
			opName = eo.getClass().getSimpleName()+topologyId;
			subOpName = parse(tree.input);
			builder.setBolt(opName, eo, Constants.PARALLELISM).fieldsGrouping(subOpName, new Fields(StormFields.id));
			return opName;

		case PROJECTION:
			ProjectionOperator po = new ProjectionOperator(tree);
			opName = po.getClass().getSimpleName()+topologyId;
			subOpName = parse(tree.input);
			builder.setBolt(opName, po, Constants.PARALLELISM).fieldsGrouping(subOpName, new Fields(StormFields.id));
			return opName;
			
		case JOIN:
			StormGrouper sg1 = new StormGrouper(tree.left_join_attribute, 1);
			String sg1Name = sg1.getClass().getSimpleName()+topologyId;
			StormGrouper sg2 = new StormGrouper(tree.right_join_attribute, 2);
			String sg2Name = sg2.getClass().getSimpleName()+topologyId;
			
			JoinOperator jo = new JoinOperator(tree);
			opName = jo.getClass().getSimpleName()+topologyId;
			subOpName = parse(tree.left_input);
			String subOpName2 = parse(tree.right_input);
			builder.setBolt(sg1Name, sg1, Constants.PARALLELISM).fieldsGrouping(subOpName, new Fields(StormFields.id));
			builder.setBolt(sg2Name, sg2, Constants.PARALLELISM).fieldsGrouping(subOpName2, new Fields(StormFields.id));
			
			builder.setBolt(opName, jo, Constants.PARALLELISM)
				.fieldsGrouping(sg1Name, new Fields(StormFields.groupKey))
				.fieldsGrouping(sg2Name, new Fields(StormFields.groupKey));
			return opName;
			
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
		
	}

}
