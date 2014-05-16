package cn.edu.zju.jsonStreamer.test;

public class StringSplit {
	public void test(){
		String msg = "1234|5678|otherThings";
		int pos1 = msg.indexOf('|');
		int pos2 = msg.indexOf('|', pos1+1);
		if(pos1 == -1 || pos2 == -1) System.err.println("wrong!");;
		long id = Long.valueOf(msg.substring(0, pos1));
		long timeStamp = Long.valueOf(msg.substring(pos1+1, pos2));
		String json = msg.substring(pos2+1);
		System.out.println("id: "+id);
		System.out.println("ts: "+timeStamp);
		System.out.println("json: "+json);
	}
	public static void main(String[] args) {
		String[] s = "an d time".split(" ");
		System.out.println(s.length);
		
		System.out.println("root, error, projection, leaf, join, groupby_aggregation, selection, expand, rangewindow, rowwindow, partitionwindow,istream, rstream, dstream".toUpperCase());
		
		System.out.println("abf".compareTo("abde"));
		
		new StringSplit().test();
	}
}
