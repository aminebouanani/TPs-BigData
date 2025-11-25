package com.example.hadoop_lab.hdfslab;

import java.io.IOException;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

public class HDFSWrite {
    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.out.println("Usage: hadoop jar /shared_volume/HDFSWrite.jar <FILE_PATH>");
            System.exit(1);
        }
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path nomcomplet = new Path(args[0]);
        if (!fs.exists(nomcomplet)) {
            try (FSDataOutputStream outStream = fs.create(nomcomplet)) {
                outStream.writeUTF("Bonjour tout le monde !");
                // outStream.writeUTF(args[1]);
            }
        }
        fs.close();
    }
}
