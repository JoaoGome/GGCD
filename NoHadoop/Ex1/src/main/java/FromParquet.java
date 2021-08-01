import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
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
    public static class FromParquetMapper extends Mapper<Void, GenericRecord,Void, Text>
    {
        @Override
        protected void map(Void key, GenericRecord value, Context context) throws IOException, InterruptedException {
            String s = "ID: " + (String) value.get("tconst");
            s += "   TYPE: " + (String) value.get("type");
            s += "   PRIMARY TITLE: " + (String) value.get("primaryT");
            s += "   ORIGINAL TITLE: " + (String) value.get("originalT");
            s += "   ADULT: " + (String) value.get("isAdult");
            s += "   START: " + (String) value.get("start");
            s += "   END: " + (String) value.get("end");
            s += "   MINUTES: " + (String) value.get("minutes");
            s += "  RATING: " + (String) value.get("rating");
            s += "   VOTES: " + (String) value.get("numberVotes");

            List <String> lista = (List) value.get("generos");
            for (String genero: lista)
                s += "   GENERO: " + genero;

            context.write(null, new Text(s));
        }
    }

    public static void main (String args []) throws Exception
    {
        Job job = Job.getInstance(new Configuration(),"FromParquet");
        job.setJarByClass(FromParquet.class);
        job.setMapperClass(FromParquetMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Void.class);
        job.setMapOutputValueClass(Text.class);

        job.setInputFormatClass(AvroParquetInputFormat.class);
        AvroParquetInputFormat.addInputPath(job, new Path("Alinea1_Output_Parquet"));

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("results"));

        job.waitForCompletion(true);
    }
}
