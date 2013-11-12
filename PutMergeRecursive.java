import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PutMergeRecursive {


        public static void main(String[] args) throws IOException {
                Path inputDir = new Path(args[0]);
                Configuration conf = new Configuration();
                FileSystem hdfs = FileSystem.get(conf);
                FileSystem local = FileSystem.getLocal(conf);
                Path hdfsFile = new Path(args[1]);
                FSDataOutputStream out = hdfs.create(hdfsFile);
                recur(inputDir, local, out);
                out.close();
        }

        public static void recur(Path inDir, FileSystem local, FSDataOutputStream out){
                try {


                        FileStatus [] inputFiles = local.listStatus(inDir);

                        for (int i = 0; i < inputFiles.length; i++) {
                                if (inputFiles[i].isDir()) {
                                        recur(inputFiles[i].getPath(), local, out);
                                }
                                else {
                                        System.out.println(inputFiles[i].getPath().getName());
                                        FSDataInputStream in = local.open(inputFiles[i].getPath());
                                        byte buffer[] = new byte[256];
                                        int byteRead = 0;
                                        while ((byteRead = in.read(buffer)) > 0) {
                                                out.write(buffer, 0, byteRead);
                                        }
                                        in.close();
                                }
                        }
                        } catch (IOException e) {
                                e.printStackTrace();
                        }


                }
        }
