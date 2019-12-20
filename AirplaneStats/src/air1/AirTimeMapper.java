package air1;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AirTimeMapper extends Mapper<Object, Text, Text, FloatWritable> {
	private Text word = new Text();
	boolean flag=true;

	public void map(Object key, Text value, Context context) throws IOException,
	InterruptedException {

		word.set(value);
		String nextLine =word.toString();
		String [] columns=nextLine.split(",");

		// 29 columnas e ignorar primera linea. Columna 13 es AirTime

		if(columns.length==29 && !columns[13].equals("AirTime") && flag && !columns[13].equals("NA"))
		{
			// Sumando vuelos
			context.write(new Text("TotalVuelos"), new FloatWritable(1));

			// Tiempo en el Aire
			context.write(new Text("AirTime"), new FloatWritable(Float.parseFloat(columns[13])));
			
		}
	}
}