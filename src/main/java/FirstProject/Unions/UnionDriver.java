package FirstProject.Unions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class UnionDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int exitCode = ToolRunner.run(new UnionDriver(), args);
		System.exit(exitCode);

	}
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub

		if (arg0.length != 3) {
			System.out.printf(
			"Usage: %s [generic options] <input dir> <output dir>\n", getClass()
			.getSimpleName());
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
			}	
		
		Path ip1 = new Path(arg0[0]);
		Path ip2 = new Path(arg0[1]);
		Path op = new Path(arg0[2]);
 		
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(UnionDriver.class);
		job.setJobName(this.getClass().getName());
		
		job.setReducerClass(MyReducer.class);
		
		//Mapper output
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		//Reducer output format
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		MultipleInputs.addInputPath(job, ip1,TextInputFormat.class, MyMapper1.class);
		MultipleInputs.addInputPath(job, ip2,TextInputFormat.class, MyMapper2.class);
		
		FileOutputFormat.setOutputPath(job, op);
		
	//	job.setNumReduceTasks(0);

		if (job.waitForCompletion(true)) {
			return 0;
		}

		return 1;
	}
}
