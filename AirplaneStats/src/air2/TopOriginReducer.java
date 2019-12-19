package air2;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class TopOriginReducer extends Reducer<Text, IntWritable, Text, IntWritable> { 
	int cash=0;
	int credit=0;
	boolean flag=true;
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws
	IOException, InterruptedException { 
		int sumVal = 0;
		for (IntWritable val : values)
		{
			sumVal+=val.get();	
		}
		context.write(key, new IntWritable(sumVal));
	}
}