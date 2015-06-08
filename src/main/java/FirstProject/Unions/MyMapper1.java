package FirstProject.Unions;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MyMapper1 extends Mapper<LongWritable, Text, Text, Text>{
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line=value.toString();
		String[] arr=line.split(",");
		String id=arr[0];
		String location=arr[2];
		String name=arr[1];
		
			context.write(new Text(id), new Text(name + location));
		
	}
}
