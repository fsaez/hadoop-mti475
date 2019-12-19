package air1;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class AirTimeReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {
	private IntWritable result = new IntWritable(); 
	float totalPassenger = 0;
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws
	IOException, InterruptedException { 
		 	int sumVal = 0;
		 	if(key.find("TotalVuelos")>0) // si TotalVuelos presente 
		 	{
		 		for (IntWritable val : values)
				{
					sumVal+=val.get();	
				}
		 		totVuelos=sumVal; 
		 		
		 	//every key is written to disk for varification
		 	context.write(key, new FloatWritable(sumVal));
		 	}
		 	else
		 	{//passanger count
		 		for (IntWritable val : values)
				{
					sumVal+=val.get();	
				}
		 		
		 	context.write(key, new FloatWritable(sumVal)); // passanger count with key write on disk
		 	context.write(new Text("Promedio_AirTime"), new FloatWritable(totVuelos/sumVal));
		 	}				
	 }
}