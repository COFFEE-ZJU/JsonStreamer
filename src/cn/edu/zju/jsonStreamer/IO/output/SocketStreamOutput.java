package cn.edu.zju.jsonStreamer.IO.output;

import java.io.IOException;
import java.io.OutputStream;

import cn.edu.zju.jsonStreamer.json.MarkedElement;


public class SocketStreamOutput implements JStreamOutput{
	private OutputStream out;
	
	public SocketStreamOutput(OutputStream outputStream){
		out = outputStream;
	}
	
	@Override
	public boolean pushNext(String ele) {
		try {
			out.write(ele.getBytes());
			out.write("\n".getBytes());
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
