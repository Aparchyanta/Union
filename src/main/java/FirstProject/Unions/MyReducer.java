package FirstProject.Unions;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, Text, Text, Text> {
	private Log ll = LogFactory.getLog(UnionDriver.class);
	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1,
			Reducer<Text, Text, Text, Text>.Context arg2) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		String records = " ";
		
		for(Text temp : arg1)
		{
			records = records + " " + temp.toString();			
		}
		ll.info("records= "+ records);
		arg2.write(arg0, new Text(records));
	
	}
}
