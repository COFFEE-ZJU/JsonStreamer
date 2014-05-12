package cn.edu.zju.jsonStreamer.IO;

import cn.edu.zju.jsonStreamer.constants.SystemErrorException;
import cn.edu.zju.jsonStreamer.json.MarkedElement;

public interface JStreamOutput {
	public boolean pushNext(MarkedElement ele) throws SystemErrorException;
	public boolean isFull();
	public void execute();
}
