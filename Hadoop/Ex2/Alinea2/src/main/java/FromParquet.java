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
import java.util.List;

public class FromParquet
{
    public static class FromParquetMapper extends Mapper<Void, GenericRecord,Text, Text>
    {
        @Override
        protected void map(Void key, GenericRecord value, Context context) throws IOException, InterruptedException {

            String type = (String) value.get("type");

            if (!type.equals("movie")) return; // se não for do tipo movies, então nao interessam para esta alinea

            else
            {
                String year = (String) value.get("start");
                String votes = (String) value.get("numberVotes");
                String id = (String) value.get("tconst");
                String nova = id + "_" + votes;

                context.write(new Text(year), new Text(nova));
            }
        }
    }

    public static class FromParquetReducer extends Reducer<Text, Text,Text, Text>
    {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long maxVotes = 0;
            String maxVotesId = null;

            for (Text value: values)
            {
                String cValue = value.toString();
                String [] bocados = cValue.split("_");
                if (Long.parseLong(bocados[1]) > maxVotes)
                {
                    maxVotes = Long.parseLong(bocados[1]);
                    maxVotesId = bocados[0];
                }

            }
            context.write(key,new Text(maxVotesId));
        }
    }

    public static void main (String args []) throws Exception
    {
        Job job = Job.getInstance(new Configuration(),"FromParquet");
        job.setJarByClass(FromParquet.class);
        job.setMapperClass(FromParquetMapper.class);
        job.setReducerClass(FromParquetReducer.class);

        job.setInputFormatClass(AvroParquetInputFormat.class);
        AvroParquetInputFormat.addInputPath(job, new Path("hdfs:///Alinea1_Output_Parquet"));

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("hdfs:///Ex2Al2results"));

        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }
}
