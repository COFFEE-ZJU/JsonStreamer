package cn.edu.zju.jsonStreamer.operators;

import cn.edu.zju.jsonStreamer.constants.Constants;
import cn.edu.zju.jsonStreamer.constants.Constants.ElementMark;
import cn.edu.zju.jsonStreamer.constants.Constants.WindowUnit;
import cn.edu.zju.jsonStreamer.constants.SystemErrorException;
import cn.edu.zju.jsonStreamer.json.MarkedElement;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonQueryTree;
import cn.edu.zju.jsonStreamer.parse.RegisterForLocal;

public class RangeWindowOperator extends OperatorStreamToRelation{
	private final long timeRange;		//time unit: millisecond  0 for now; -1 for unbounded
	public RangeWindowOperator(JsonQueryTree tree, RegisterForLocal register, int opNum) throws SystemErrorException{
		super(tree, register, opNum);
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
	protected void process(MarkedElement markedElement) throws SystemErrorException {
		if(timeRange != -1){
			long cutTime = markedElement.timeStamp - timeRange;
			MarkedElement me;
			while(! synopsis.isEmpty()){
				me = synopsis.peek();
				if(me.timeStamp <= cutTime){
					if(bufferedNum == synopsis.size()){
						synopsis.poll();
						bufferedNum --;
					}
					else{
						synopsis.poll();
						output(new MarkedElement(me.element, me.id, ElementMark.MINUS, markedElement.timeStamp));
					}
				}
				else break;
			}
		}
		
		synopsis.add(markedElement);
		bufferedNum ++;
//		if(Constants.DEBUG) System.out.println("rangeWin"+hashCode()+": ");
	}

}
