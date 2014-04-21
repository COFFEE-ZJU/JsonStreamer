package operators;

import json.MarkedElement;
import jsonAPI.JsonQueryTree;
import constants.Constants;
import constants.Constants.ElementMark;
import constants.Constants.WindowUnit;
import constants.SystemErrorException;

public class RangeWindowOperator extends OperatorStreamToRelation{
	private final long timeRange;		//time unit: millisecond  0 for now; -1 for unbounded
	public RangeWindowOperator(JsonQueryTree tree){
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
					throw new SystemErrorException();
				}
			}
			else if(times[0].equals(WindowUnit.UNBOUNDED)) timeRange = -1;
			else throw new SystemErrorException();
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
				throw new SystemErrorException();
			}
		}
		else throw new SystemErrorException();
	}

	@Override
	protected void process(MarkedElement markedElement) {
		if(timeRange != -1){
			long cutTime = markedElement.timeStamp - timeRange;
			MarkedElement me;
			while(! synopsis.isEmpty()){
				me = synopsis.peek();
				if(me.timeStamp <= cutTime){
					synopsis.poll();
					outputQueue.add(new MarkedElement(me.element, me.id, ElementMark.MINUS, markedElement.timeStamp));
				}
				else break;
			}
		}
		
		synopsis.add(markedElement);
		outputQueue.add(markedElement);
	}

}
