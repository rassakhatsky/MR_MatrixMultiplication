package edu.stevens.bia678.matrix;

import java.io.*;

/**
 * Created by rassakhatsky on 10/02/15.
 */
public class Matrix {
    static String m1 = "matrix1";
    static String m2 = "matrix2";
    static String FS_FOLDER = "/home/cloudera/Desktop/TempJavaProject/src/edu/stevens/bia678/matrix/files/";
    static int MAX_COLUMNS = 50;

    private double[][] matrix;

    private int width;
    private int height;

    public static void main(String[] args) {
        for (int i = 3; i < MAX_COLUMNS; i++) {
            String matrixName = "matrix_" + i + ".txt";
            createRandomMatrix(FS_FOLDER, matrixName, i, i, i);
        }
    }

    public double[][] getMatrix() {
        return matrix;
    }

    public void setMatrix(double[][] matrix) {
        width = matrix[0].length;
        height = matrix.length;
        this.matrix = matrix;
    }

    public void makeItRandom(int w, int h) {
        createRandomMatrix(w, h);
    }


    /**
     * Create a random matrix w-by-h size
     *
     * @param w - width
     * @param h - height
     * @return
     */
    private void createRandomMatrix(int w, int h) {
        matrix = new double[h][w];
        for (int i = 0; i < h; i++)
            for (int j = 0; j < w; j++)
                matrix[i][j] = (int) (Math.random() * 100);

        height = matrix.length;
        width = matrix[0].length;
    }

    /**
     * Matrix multiplication (matrix1 * matrix2)
     *
     * @param matrix1
     * @param matrix2
     * @return
     */
    public double[][] multiplyMatrices(double[][] matrix1, double[][] matrix2) {
        int h1 = matrix1.length;
        int w1 = matrix1[0].length;
        int h2 = matrix2.length;
        int w2 = matrix2[0].length;

        if (w1 != h2) throw new RuntimeException("FAIL");
        double[][] newMatrix = new double[h1][w2];

        for (int i = 0; i < h1; i++)
            for (int j = 0; j < w2; j++)
                for (int k = 0; k < w1; k++)
                    newMatrix[i][j] += (matrix1[i][k] * matrix2[k][j]);
        return newMatrix;
    }

    public void saveMatix4Multiplication(Matrix[] myMatrix, String fileName) {
        FileOutputStream fop;
        File file;
        StringBuilder fileContent = new StringBuilder();
        Writer writer = null;

        for (int m = 0; m < myMatrix.length; m++) {
            for (int i = 0; i < (double) myMatrix[m].getMatrix().length; i++) {
                String line = m + "," + i + ",";
                for (int j = 0; j < myMatrix[m].getMatrix()[0].length; j++) {
                    fileContent.append(line + j + "," + myMatrix[m].getMatrix()[i][j] + "\n");
                }
            }
        }

        try {
            file = new File(fileName);
            if (!file.exists()) {
                file.createNewFile();
            }
            fop = new FileOutputStream(file);
            writer = new OutputStreamWriter(fop, "UTF-8");
            writer.write(fileContent.toString());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void createRandomMatrix(String Folder, String FaleName, int x, int y, int z) {
        Matrix matrix1 = new Matrix();
        Matrix matrix2 = new Matrix();

        matrix1.makeItRandom(x, y);
        matrix2.makeItRandom(y, z);

        Matrix[] m = {matrix1, matrix2};
        new Matrix().saveMatix4Multiplication(m, Folder + FaleName);

    }
}