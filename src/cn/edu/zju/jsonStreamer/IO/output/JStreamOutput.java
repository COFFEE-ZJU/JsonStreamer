package cn.edu.zju.jsonStreamer.IO.output;

import cn.edu.zju.jsonStreamer.constants.SystemErrorException;
import cn.edu.zju.jsonStreamer.json.MarkedElement;

public interface JStreamOutput {
	public boolean pushNext(String ele) throws SystemErrorException;
	public boolean isFull();
	public void execute();
}
