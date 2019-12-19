package air1;
import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AirTimeCombiner extends Reducer<Text, FloatWritable, Text, FloatWritable> {
	private FloatWritable result = new FloatWritable();
	public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws
	IOException, InterruptedException {
		// combiner for each class is almost same only output value may be change as per requirement
		int sum = 0;
		for (FloatWritable val : values) {
			sum += val.get();
		}
		result.set(sum);
		context.write(key, result);
	}
}