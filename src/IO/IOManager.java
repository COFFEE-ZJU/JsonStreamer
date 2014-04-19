package IO;

import java.util.HashMap;
import java.util.Map;

import constants.SystemErrorException;

public class IOManager {
	private static IOManager ioManager = null;
	private Map<String, JStreamInput> inputStreamMap;
	private Map<String, JStreamOutput> outputStreamMap;
	private IOManager(){
		inputStreamMap = new HashMap<String, JStreamInput>();
		outputStreamMap = new HashMap<String, JStreamOutput>();
	}
	
	public static IOManager getInstance(){
		if(ioManager == null) ioManager = new IOManager();
		
		return ioManager;
	}
	public JStreamOutput getOutputStreamByName(String streamName){
		if(! outputStreamMap.containsKey(streamName))
			throw new SystemErrorException("stream: "+streamName+" not exist!");
		
		return outputStreamMap.get(streamName);
	}
	public JStreamInput getInputStreamByName(String streamName){
		if(! inputStreamMap.containsKey(streamName))
			throw new SystemErrorException("stream: "+streamName+" not exist!");
		
		return inputStreamMap.get(streamName);
	}
	
	public boolean registerInputStream(String streamName, JStreamInput inputStream){
		if(inputStreamMap.containsKey(streamName))
			return false;
		
		inputStreamMap.put(streamName, inputStream);
		return true;
	}
	
	public boolean registerOutputStream(String streamName, JStreamOutput outputStream){
		if(outputStreamMap.containsKey(streamName))
			return false;
		
		outputStreamMap.put(streamName, outputStream);
		return true;
	}
}
