package IO;

import java.io.IOException;
import java.io.OutputStream;

import json.MarkedElement;

public class SocketStreamOutput implements JStreamOutput{
	private OutputStream out;
	
	public SocketStreamOutput(OutputStream outputStream){
		out = outputStream;
	}
	
	@Override
	public boolean pushNext(MarkedElement ele) {
		try {
			out.write(ele.element.jsonElement.toString().getBytes());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return true;
	}

	@Override
	public boolean isFull() {
		return false;
	}

	@Override
	public void execute() {
	}

}
