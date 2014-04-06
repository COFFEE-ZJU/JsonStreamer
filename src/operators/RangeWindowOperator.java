package operators;

import jsonAPI.JsonQueryTree;
import constants.Constants.WindowUnit;

public class RangeWindowOperator extends Operator{
	private final long timeRange;		//quantity: second  0 for now; -1 for unbounded
	public RangeWindowOperator(JsonQueryTree tree){
		super(tree);
		String time = (String)tree.windowsize;
		String[] times = time.split(" ");
		if(times.length == 1){
			if(times[0].equals(WindowUnit.NOW)) timeRange = 0;
			else if(times[0].equals(WindowUnit.UNBOUNDED)) timeRange = -1;
			else timeRange = 0;
		}
		else if(times.length == 2){
			int num = Integer.valueOf(times[0]);
			switch (times[1]) {
			case WindowUnit.SECONDS:
				timeRange = num;
				break;
			case WindowUnit.MINUTES:
				timeRange = num * 60;
				break;
			case WindowUnit.HOURS:
				timeRange = num * 3600;
				break;
			case WindowUnit.DAYS:
				timeRange = num * 3600 *24;
				break;

			default:
				timeRange = 0;
				break;
			}
		}
		else timeRange = 0;
	}
	@Override
	public void execute() {
		// TODO Auto-generated method stub
		
	}

}
