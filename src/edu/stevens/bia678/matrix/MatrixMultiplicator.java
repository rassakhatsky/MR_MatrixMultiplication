package edu.stevens.bia678.matrix;


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

import java.io.IOException;
import java.util.HashMap;


/**
 * Created by rassakhatsky on 10/02/15.
 */
public class MatrixMultiplicator {
    static String X = "x";
    static String Y = "y";
    static String Z = "z";

    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
        String x, y, z;
        Configuration conf;
        Job job;

        // TODO сделать нормальную поодержку матриц любого размера
        x = args[0].substring(args[0].lastIndexOf("_") + 1, args[0].lastIndexOf("."));
        y = x;
        z = x;

        // We multiply 2 matricies X-Y to Y-Z
        conf = new Configuration();
        conf.set(X, x);
        conf.set(Y, y);
        conf.set(Z, z);

        job = Job.getInstance(conf);
        job.setJobName("MatrixMatrixMultiplicationOneStep");
        //Job job = new Job(conf, "MatrixMatrixMultiplicationOneStep");
        job.setJarByClass(MatrixMultiplicator.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MyMap.class);
        job.setReducerClass(MyReduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

    public static class MyMap extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int x = Integer.parseInt(conf.get(X));
            int z = Integer.parseInt(conf.get(Z));
            String line = value.toString();
            String[] indicesAndValue = line.split(",");

            Text outputKey = new Text();
            Text outputValue = new Text();

            if (indicesAndValue[0].equals("0")) { //If  it's matrix 1
                for (int k = 0; k < z; k++) {
                    outputKey.set(indicesAndValue[1] + "," + k);
                    outputValue.set("0," + indicesAndValue[2] + "," + indicesAndValue[3]);
                    context.write(outputKey, outputValue);
                }
            } else { //If it's matrix 2
                for (int i = 0; i < x; i++) {
                    outputKey.set(i + "," + indicesAndValue[2]);
                    outputValue.set("1," + indicesAndValue[1] + "," + indicesAndValue[3]);
                    context.write(outputKey, outputValue);
                }
            }
        }
    }

    public static class MyReduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] value;
            HashMap<Integer, Float> hashA = new HashMap<Integer, Float>();
            HashMap<Integer, Float> hashB = new HashMap<Integer, Float>();

            for (Text val : values) {
                value = val.toString().split(",");
                if (value[0].equals("0")) {
                    hashA.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
                } else {
                    hashB.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
                }
            }

            int y = Integer.parseInt(context.getConfiguration().get(Y));
            float result = 0.0f;
            float a_ij;
            float b_jk;
            for (int j = 0; j < y; j++) {
                a_ij = hashA.containsKey(j) ? hashA.get(j) : 0.0f;
                b_jk = hashB.containsKey(j) ? hashB.get(j) : 0.0f;
                result += a_ij * b_jk;
            }
            if (result != 0.0f) {
                context.write(null, new Text(key.toString() + "," + Float.toString(result)));
            }
        }
    }
}