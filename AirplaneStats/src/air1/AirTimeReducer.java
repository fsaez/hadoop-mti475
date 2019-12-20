package air1;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class AirTimeReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
	private FloatWritable result = new FloatWritable(); 
	float totalVuelos = 0;
	float totalAirTime = 0;
	boolean flag=true;
	
	public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws
	IOException, InterruptedException { 
		float sumVal = 0;
		
		for (FloatWritable val : values)
		{
			sumVal+=val.get();	
		}
		context.write(key, new FloatWritable(sumVal));
		
		if(key.equals(new Text("TotalVuelos")))
		{		
			totalVuelos=sumVal;
		}
		if(key.equals(new Text("AirTime")))
		{
			totalAirTime=sumVal;
		}
		if(flag && totalVuelos>0 && totalAirTime>0)
		{
			flag=false;
			context.write(new Text("AirTime Promedio"), new FloatWritable(totalAirTime/totalVuelos));
		}					
	 }
}