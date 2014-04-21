package IO;

import java.util.HashMap;
import java.util.Map;

import wrapper.WrapperManager;

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
	public JStreamInput getInputStreamByName(String wrapperName){
		if(! inputStreamMap.containsKey(wrapperName)){
			JStreamInput stream = WrapperManager.getStreamInputByWrapper(wrapperName);
			inputStreamMap.put(wrapperName, stream);
		}
		
		return inputStreamMap.get(wrapperName);
	}
	
	public boolean registerInputStream(String wrapperName, JStreamInput inputStream){
		if(inputStreamMap.containsKey(wrapperName))
			return false;
		
		inputStreamMap.put(wrapperName, inputStream);
		return true;
	}
	
	public boolean registerOutputStream(String streamName, JStreamOutput outputStream){
		if(outputStreamMap.containsKey(streamName))
			return false;
		
		outputStreamMap.put(streamName, outputStream);
		return true;
	}
}
