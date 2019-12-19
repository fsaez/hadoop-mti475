package air1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AirTimeJobControl {
	 public static void main(String[] args) throws Exception {
		 
		 Configuration conf = new Configuration();
		 Job job = new Job();
		 job.setJarByClass(Nyc1JobControl.class);
		 job.setMapperClass(Nyc1Mapper.class);
		 job.setCombinerClass(Nyc1Combiner.class);
		 job.setReducerClass(Nyc1Reducer.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(IntWritable.class);
		 
		 FileInputFormat.addInputPath(job, new Path(args[0]));
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 System.exit(job.waitForCompletion(true) ? 0 : 1);
		 
		 }
}