package utils;

public class TimeStampGenerator {
	private TimeStampGenerator(){};
	private static final long startTime = System.currentTimeMillis();
	public static long getCurrentTimeStamp(){
		return System.currentTimeMillis() - startTime;
	}
}
