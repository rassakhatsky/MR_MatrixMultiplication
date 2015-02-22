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
import java.net.URISyntaxException;
import java.util.HashMap;


/**
 * Created by rassakhatsky on 10/02/15.
 */
public class MatrixMultiplicator {
    static String X = "x";
    static String Y = "y";
    static String Z = "z";
    static String HADOOP_URI = "hdfs://localhost:8020";
    static String HADOOP_FOLDER = "/user/cloudera/tmp/matrix/";
    static String FS_FOLDER = "/home/cloudera/Desktop/TempJavaProject/src/edu/stevens/bia678/matrix/";

    static String MatricesFile = "matrix.txt";
    static boolean DEBUG = false;

    public static void main(String[] args) throws ClassNotFoundException, InterruptedException, IOException, URISyntaxException {
        String x, y, z;
        if (DEBUG) {
            args = new String[2];
            args[0] = FS_FOLDER + MatricesFile;
            args[1] = FS_FOLDER + "Results";

            double[][] myMatrix1 = {{1, 2, 3}, {4, 5, 7}, {7, 8, 9}};
            double[][] myMatrix2 = {{10, 11, 12}, {13, 14, 15}, {16, 17, 18}};

            Matrix matrix1 = new Matrix();
            Matrix matrix2 = new Matrix();

            matrix1.setMatrix(myMatrix1);
            matrix2.setMatrix(myMatrix2);
            Matrix[] m = {matrix1, matrix2};
            new Matrix().saveMatix4Multiplication(m, args[0]);
            x = "3";
            y = x;
            z = x;
        } else {
            x = args[0].substring(args[0].lastIndexOf("_") + 1, args[0].lastIndexOf("."));
            y = x;
            z = x;
        }


        // We multiply 2 matricies X by Y to Y by Z

        Configuration conf = new Configuration();
        conf.set(X, x);
        conf.set(Y, y);
        conf.set(Z, z);

        Job job = Job.getInstance(conf);
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

    private static void CreateRandomMatrix(String Folder) {
        Matrix matrix1 = new Matrix();
        Matrix matrix2 = new Matrix();

        matrix1.makeItRandom(3, 3);
        matrix2.makeItRandom(3, 3);

        Matrix[] m = {matrix1, matrix2};
        new Matrix().saveMatix4Multiplication(m, Folder + MatricesFile);
    }
}