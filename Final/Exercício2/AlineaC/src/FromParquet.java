import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FromParquet
{
    public static class FromParquetMapper extends Mapper<Void, GenericRecord,Text, Text>
    {
        @Override
        protected void map(Void key, GenericRecord value, Context context) throws IOException, InterruptedException {

            String type = (String) value.get("type");
            float rating = Float.parseFloat((String) value.get("rating"));

            if (!type.equals("movie") || rating <= 8.0) return; // se não for do tipo movies ou rating for menos que 8 nao interessam para esta query

            else
            {
                String year = (String) value.get("start");

                List <String> lista = (List) value.get("generos");

                for (String genero: lista)
                    context.write(new Text(year), new Text(genero));
            }
        }
    }

    public static class FromParquetReducer extends Reducer<Text, Text,Text, Text>
    {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            HashMap<String,Integer> contagem = new HashMap<>();
            int maxCount = 0;
            String generoMax = null;

            for (Text value: values)
            {
                if (!value.toString().equals("\\N"))
                {
                    if (!contagem.containsKey(value.toString()))
                        contagem.put(value.toString(),1);
                    else
                    {
                        int count = contagem.get(value.toString());
                        contagem.put(value.toString(),count+1);
                    }
                }

            }

            for (Map.Entry<String, Integer> entry : contagem.entrySet())
            {
                if (entry.getValue() > maxCount)
                {
                    maxCount = entry.getValue();
                    generoMax = entry.getKey();
                }
            }
            String s = generoMax + " " + maxCount;
            context.write(key,new Text(s));
        }
    }

    public static void main (String args []) throws Exception
    {
        Job job = Job.getInstance(new Configuration(),"FromParquet");
        job.setJarByClass(FromParquet.class);
        job.setMapperClass(FromParquetMapper.class);
        job.setReducerClass(FromParquetReducer.class);

        job.setInputFormatClass(AvroParquetInputFormat.class);
        AvroParquetInputFormat.addInputPath(job, new Path("Alinea1_Output_Parquet"));

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("results"));

        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }
}
