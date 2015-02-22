package edu.stevens.bia678.matrix.test;

import edu.stevens.bia678.matrix.MatrixMultiplicator;

import java.io.IOException;

/**
 * Created by rassakhatsky on 21/02/15.
 */
public class Test {
    static String FS_FOLDER = "/home/cloudera/Desktop/TempJavaProject/src/edu/stevens/bia678/matrix/";
    static String MATRIX = "matrix.txt";

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        test1(args);
    }

    private static void test1(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        String x, y, z;
        args = new String[2];
        args[0] = FS_FOLDER + MATRIX;
        args[1] = FS_FOLDER + "Results";

        double[][] myMatrix1 = {{1, 2, 3}, {4, 5, 7}, {7, 8, 9}};
        double[][] myMatrix2 = {{10, 11, 12}, {13, 14, 15}, {16, 17, 18}};

        Matrix matrix1 = new Matrix();
        Matrix matrix2 = new Matrix();

        matrix1.setMatrix(myMatrix1);
        matrix2.setMatrix(myMatrix2);
        Matrix[] m = {matrix1, matrix2};
        new Matrix().saveMatix4Multiplication(m, args[0]);

        MatrixMultiplicator mapreduce = new MatrixMultiplicator();
        mapreduce.main(args);
    }
}
