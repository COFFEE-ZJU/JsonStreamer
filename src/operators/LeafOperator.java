package operators;

import json.Element;
import jsonAPI.JsonQueryTree;
import IO.IOManager;
import IO.JStreamInput;

public class LeafOperator extends Operator{
	private boolean isMaster = false;
	private JStreamInput inputStream;
	
	public LeafOperator(JsonQueryTree tree){
		super(tree);
		JStreamInput inputStream = IOManager.getInstance().getInputStreamByName(tree.stream_source);
		setInputStream(inputStream, tree.is_master);
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
