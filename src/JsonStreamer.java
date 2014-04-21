import operators.Operator;
import IO.FileStreamOutput;
import IO.JStreamOutput;
import IO.ScreenStreamOutput;
import parse.APIParser;
import parse.QueryParser;
import scheduler.Scheduler;
import utils.SchemaServer;


public class JsonStreamer {

	public static void main(String[] args) {
		new SchemaServer().start();
		String API1 = QueryParser.parseToAPI("query_1");
		String API2 = QueryParser.parseToAPI("query_2");
		System.out.println(API1);
		System.out.println(API2);
		
		JStreamOutput outputStream2 = new ScreenStreamOutput();
		JStreamOutput outputStream1 = new FileStreamOutput("output_1");
		Scheduler scheduler = new Scheduler(new APIParser().parse(API1, outputStream1));
		scheduler.addOperators(new APIParser().parse(API2, outputStream2));
		outputStream1.execute();
		outputStream2.execute();
		Operator op;
		while(true){
			op = scheduler.getNextOperator();
			op.execute();
		}
//		System.out.println("end");
	}

}
