package air2;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopOriginMapper extends Mapper<Object, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(Object key, Text value, Context context) throws IOException,
	InterruptedException {
		
			word.set(value);
			String nextLine =word.toString();
			
			String [] columns=nextLine.split(",");
			
			//Cantidad de columnas 29 e ignorar primera linea	
					if(columns.length==29 && !columns[16].equals("Origin"))
					{
						// Columna 16 es Origen
						context.write(new Text(columns[16]), one);
					}
		}
}