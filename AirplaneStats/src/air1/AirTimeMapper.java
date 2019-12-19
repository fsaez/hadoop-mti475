package air1;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AirTimeMapper extends Mapper<Object, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(Object key, Text value, Context context) throws IOException,
	InterruptedException {

		word.set(value);
		String nextLine =word.toString();
		String [] columns=nextLine.split(",");

		// 29 columnas y comprobacion minima de CSV
		if(columns.length==29 && columns[0].equals("Year"))
		{
			// Sumando vuelos
			context.write(new Text("TotalVuelos"), new IntWritable(1));

			// Tiempo en el Aire
			context.write(new Text("AirTime"), new IntWritable(Integer.parseInt(columns[6])));

		}
	}
}