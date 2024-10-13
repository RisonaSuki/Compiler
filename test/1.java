import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.net.URI;

public class HdfsFileOperations {

    public static void uploadFile(String localFilePath, String hdfsFilePath, boolean append) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);
        Path hdfsPath = new Path(hdfsFilePath);
        
        if (fs.exists(hdfsPath)) {
            if (append) {
                // 追加到文件
                FSDataOutputStream out = fs.append(hdfsPath);
                FileInputStream in = new FileInputStream(localFilePath);

                byte buffer[] = new byte[256];
                int bytesRead = 0;
                while ((bytesRead = in.read(buffer)) > 0) {
                    out.write(buffer, 0, bytesRead);
                }

                in.close();
                out.close();
            } else {
                // 覆盖文件
                fs.copyFromLocalFile(false, true, new Path(localFilePath), hdfsPath);
            }
        } else {
            // 文件不存在，直接上传
            fs.copyFromLocalFile(new Path(localFilePath), hdfsPath);
        }
        fs.close();
    }

    public static void main(String[] args) throws Exception {
        // 这里假设命令行参数传递localFilePath, hdfsFilePath, appendFlag
        String localFilePath = args[0];
        String hdfsFilePath = args[1];
        boolean append = Boolean.parseBoolean(args[2]);
        
        uploadFile(localFilePath, hdfsFilePath, append);
    }
}
