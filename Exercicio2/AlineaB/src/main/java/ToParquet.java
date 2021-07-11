import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class ToParquet
{
    public static Schema getSchema() throws IOException
    {
        InputStream is = new FileInputStream("schema.parquet");
        String ps = new String(is.readAllBytes());
        MessageType mt = MessageTypeParser.parseMessageType(ps);
        return new AvroSchemaConverter().convert(mt);
    }

    public static class ToParquetBasics extends Mapper <LongWritable, Text, Text, Text>
    {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0) return; // ignorar o cabeçalho

            String info [] = value.toString().split("\\t");

            if (info.length != 9) return;

            StringBuilder sb = new StringBuilder();
            sb.append(info[1] + "_"); // tipo
            sb.append(info[2] + "_"); // primary title
            sb.append(info[3] + "_"); // original title
            sb.append(info[4] + "_"); // isAdult
            sb.append(info[5] + "_"); // startYear
            sb.append(info[6] + "_"); // endYear
            sb.append(info[7] + "_"); // minutes

            boolean counter = false;
            for (String genre: info[8].split(","))
            {
                if (counter) sb.append(",");
                sb.append(genre);
                counter = true;
            }

            context.write(new Text(info[0]), new Text(sb.toString()));

        }
    }

    public static class ToParquetRatings extends Mapper <LongWritable, Text, Text, Text>
    {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0) return; //ignorar cabeçalhos

            String [] info = value.toString().split("\\t");
            StringBuilder sb = new StringBuilder();

            sb.append("Ratings" + "_"); // indicar no reducer que este value vem de qual dos mappers
            sb.append(info[1] + "_"); // rating
            sb.append(info[2] + "_"); // votes

            context.write(new Text(info[0]), new Text(sb.toString()));
        }
    }

    public static class ToParquetReducer extends Reducer<Text, Text, Void, GenericRecord>
    {
        private Schema schema;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.schema = getSchema();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            boolean ratings = false;
            boolean basics = false;

            GenericRecord record = new GenericData.Record(this.schema);
            record.put("tconst",key);

            for (Text value: values)
            {
                List <String> generos = new ArrayList<>();
                String [] info = value.toString().split("_");

                if (!info[0].equals("Ratings"))
                {
                    record.put("type",info[0]);
                    record.put("primaryT",info[1]);
                    record.put("originalT",info[2]);
                    record.put("isAdult",info[3]);
                    record.put("start",info[4]);
                    record.put("end",info[5]);
                    record.put("minutes",info[6]);

                    for (String genre: info[7].split(","))
                        generos.add(genre);

                    record.put("generos",generos);
                    basics = true;
                }
                else
                {
                    record.put("rating",info[1]);
                    record.put("numberVotes", info[2]);
                    ratings = true;
                }
            }

            if (basics && ratings)
                context.write(null, record);
            else return;

        }
    }

    public static void main (String args []) throws Exception
    {

        Job job = Job.getInstance(new Configuration(), "ToParquetA1");
        job.setJarByClass(ToParquet.class);

        //input
        job.setInputFormatClass(TextInputFormat.class);
        MultipleInputs.addInputPath(job,new Path("title.basics.tsv"),
                TextInputFormat.class, ToParquetBasics.class);

        MultipleInputs.addInputPath(job,new Path("title.ratings.tsv"),
                TextInputFormat.class, ToParquetRatings.class);

        job.setReducerClass(ToParquetReducer.class);

        //output
        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(GenericRecord.class);
        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setSchema(job, getSchema());
        FileOutputFormat.setOutputPath(job,new Path("Alinea1_Output_Parquet"));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }
}










