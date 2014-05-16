package cn.edu.zju.jsonStreamer.operators.storm;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import com.google.gson.Gson;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cn.edu.zju.jsonStreamer.constants.Constants;
import cn.edu.zju.jsonStreamer.constants.Constants.ElementMark;
import cn.edu.zju.jsonStreamer.constants.Constants.StormFields;
import cn.edu.zju.jsonStreamer.constants.Constants.WindowUnit;
import cn.edu.zju.jsonStreamer.constants.SystemErrorException;
import cn.edu.zju.jsonStreamer.json.MarkedElement;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonQueryTree;

public class RangeWindowOperator extends Operator{
	private final long timeRange;		//time unit: millisecond  0 for now; -1 for unbounded
	private Queue<Long> synopsis;
	private Queue<Long> timeSynopsis;
	
	protected Gson gson;
	
	public RangeWindowOperator(JsonQueryTree tree) throws SystemErrorException{
		super(tree);
		
		String time = (String)tree.windowsize;
		String[] times = time.split(" ");
		if(times.length == 1){
			if(times[0].equals(WindowUnit.NOW)){
				switch (Constants.TIME_UNIT_FOR_NOW) {
				case MILLISECOND:
					timeRange = 1;
					break;
				case SECOND:
					timeRange = 1000;
					break;
				case MINUTE:
					timeRange = 60 * 1000;
					break;
				default:
					throw new SystemErrorException("wrong API");
				}
			}
			else if(times[0].equals(WindowUnit.UNBOUNDED)) timeRange = -1;
			else throw new SystemErrorException("wrong API");
		}
		else if(times.length == 2){
			int num = Integer.valueOf(times[0]);
			switch (times[1]) {
			case WindowUnit.SECONDS:
				timeRange = num * 1000;
				break;
			case WindowUnit.MINUTES:
				timeRange = num * 60 * 1000;
				break;
			case WindowUnit.HOURS:
				timeRange = num * 3600 * 1000;
				break;
			case WindowUnit.DAYS:
				timeRange = num * 3600 *24 * 1000;
				break;

			default:
				throw new SystemErrorException("wrong API");
			}
		}
		else throw new SystemErrorException("wrong API");
	}

	@Override
    public void prepare(Map stormConf, TopologyContext context) {
		gson = new Gson();
		synopsis = new LinkedList<Long>();
		timeSynopsis = new LinkedList<Long>();
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
//		MarkedElement me = (MarkedElement)input.getValueByField(StormFields.markedElement);
		MarkedElement me = gson.fromJson(input.getStringByField(StormFields.markedElement), MarkedElement.class);
		if(me.mark == ElementMark.MINUS) throw new RuntimeException("stream element can't be MINUS marked");
		if(timeRange != -1){
			long cutTime = me.timeStamp - timeRange;
			long eleIdToDelete;
			long timeStamp;
			while(! synopsis.isEmpty()){
				timeStamp = timeSynopsis.peek();
				if(timeStamp <= cutTime){
					eleIdToDelete = synopsis.poll();
					timeSynopsis.poll();
					collector.emit(new Values(eleIdToDelete, 
							gson.toJson(new MarkedElement(null, eleIdToDelete, ElementMark.MINUS, me.timeStamp))));
				}
				else break;
			}
		}
		synopsis.add(me.id);
		timeSynopsis.add(me.timeStamp);
		collector.emit(input.getValues());
	}
}
