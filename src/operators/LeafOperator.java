package operators;

import json.Element;
import IO.JStreamInput;

public class LeafOperator extends Operator{
	private boolean isMaster = false;
	private JStreamInput inputStream;
	
	public LeafOperator(JStreamInput stream, boolean isMasterStream){
		setInputStream(stream, isMasterStream);
	}
	
	@Override
	public void execute() {
		// TODO Auto-generated method stub
		
	}

	public void setInputStream(JStreamInput stream, boolean isMasterStream){
		inputStream = stream;
		isMaster = isMasterStream;
	}
}
