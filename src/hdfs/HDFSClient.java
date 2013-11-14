package hdfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.security.UserGroupInformation;

public class HDFSClient {
	static Configuration conf;

	public void addFile(String source, String dest) throws IOException {

		FileSystem fileSystem = FileSystem.get(conf);

		// Get the filename out of the file path
		String filename = source.substring(source.lastIndexOf('/') + 1, source
				.length());

		// Create the destination path including the filename.
		if (dest.charAt(dest.length() - 1) != '/') {
			dest = dest + "/" + filename;
		} else {
			dest = dest + filename;
		}

		// Check if the file already exists
		Path path = new Path(dest);
		if (fileSystem.exists(path)) {
			System.out.println("File " + dest + " already exists");
			return;
		}

		// Create a new file and write data to it.
		FSDataOutputStream out = fileSystem.create(path);
		InputStream in = new BufferedInputStream(new FileInputStream(new File(
				source)));

		byte[] b = new byte[1024];
		int numBytes = 0;
		while ((numBytes = in.read(b)) > 0) {
			out.write(b, 0, numBytes);
		}

		// Close all the file descripters
		in.close();
		out.close();
		fileSystem.close();
	}
	public void addObject(Object obj, String dest, String filename) throws IOException {

		FileSystem fileSystem = FileSystem.get(conf);


		// Create the destination path including the filename.
		if (dest.charAt(dest.length() - 1) != '/') {
			dest = dest + "/" + filename;
		} else {
			dest = dest + filename;
		}

		// Check if the file already exists
		Path path = new Path(dest);
		if (fileSystem.exists(path)) {
			System.out.println("File " + dest + " already exists");
			return;
		}
		
		// Create a new file and write data to it.
		FSDataOutputStream out = fileSystem.create(path);
		InputStream in = new BufferedInputStream(new ByteArrayInputStream(SerializeHelper.serialize(obj)));

		byte[] b = new byte[1024];
		int numBytes = 0;
		while ((numBytes = in.read(b)) > 0) {
			out.write(b, 0, numBytes);
		}

		// Close all the file descripters
		in.close();
		out.close();
		fileSystem.close();
	}

	public void getHostnames() throws IOException {
		FileSystem fs = FileSystem.get(conf);
		DistributedFileSystem hdfs = (DistributedFileSystem) fs;
		DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();

		String[] names = new String[dataNodeStats.length];
		for (int i = 0; i < dataNodeStats.length; i++) {
			names[i] = dataNodeStats[i].getHostName();
			System.out.println((dataNodeStats[i].getHostName()));
		}
	}

	public void mkdir(String dir) throws IOException {

		FileSystem fileSystem = FileSystem.get(conf);

		Path path = new Path(dir);
		if (fileSystem.exists(path)) {
			System.out.println("Dir " + dir + " already exists!");
			return;
		}

		fileSystem.mkdirs(path);

		fileSystem.close();
	}
	public void copyMerge(String sourceDir, String destFile) throws IOException{
		FileSystem fileSystem = FileSystem.get(conf);
		FileUtil.copyMerge(fileSystem, new Path(sourceDir), fileSystem, new Path(destFile),true, conf, null);

	}
	public void delete(String path) throws IOException{
		FileSystem fileSystem = FileSystem.get(conf);
		fileSystem.delete(new Path(path), true);
	}
	public void readFile(String file) throws IOException {
		FileSystem fileSystem = FileSystem.get(conf);

		Path path = new Path(file);
		if (!ifExists(path)) {
			System.out.println("File " + file + " does not exists");
			return;
		}

		FSDataInputStream in = fileSystem.open(path);

		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line = null;
		while((line = br.readLine())!= null){
			System.out.println(line);
		}
		in.close();
		br.close();
		fileSystem.close();
	}
	public void readObject(String file) throws IOException, ClassNotFoundException {
		FileSystem fileSystem = FileSystem.get(conf);

		Path path = new Path(file);
		if (!fileSystem.exists(path)) {
			System.out.println("File " + file + " does not exists");
			return;
		}

		FSDataInputStream in = fileSystem.open(path);
		Object obj = SerializeHelper.deserialize(in) ;
		System.out.println(obj);
		in.close();
		fileSystem.close();
	}

	public boolean ifExists(Path source) throws IOException {

		FileSystem hdfs = FileSystem.get(conf);
		boolean isExists = hdfs.exists(source);
		return isExists;
	}
	public void listContent(String path){
		 try{
             FileSystem fs = FileSystem.get(conf);
             FileStatus[] status = fs.listStatus(new Path(path));  // you need to pass in your hdfs path
             for (int i=0;i<status.length;i++){
            	 if(!status[i].isDir()){
            		 System.out.println("\n\n**** File : "+status[i].getPath()+" *****");
            		 System.out.println("--------- Contents ---------");
                     BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
                     String line;
                     line=br.readLine();
                     while (line != null){
                             System.out.println(line);
                             line=br.readLine();
                     }
                     System.out.println("--------- End ---------");
            	 }else{
            		 System.out.println("\n\n**** Direcory : "+status[i].getPath()+" *****");
            	 }
             }
     }catch(Exception e){
             System.out.println("File not found");
     }
	}
	public static void main(String a[]) {
		 UserGroupInformation ugi
         = UserGroupInformation.createRemoteUser("root");

		 try {
		
		
			ugi.doAs(new PrivilegedExceptionAction<Void>() {

                public Void run() throws Exception {

                	conf = new Configuration();
            		conf.set("fs.default.name","hdfs://192.168.1.149:9000");
            		conf.set("hadoop.job.ugi", "root");
//            		conf.addResource(new Path("/usr/local/hadoop-1.0.3/conf/core-site.xml"));
//            		conf.addResource(new Path("/usr/local/hadoop-1.0.3/conf/hdfs-site.xml"));
//            		conf.addResource(new Path("/usr/local/hadoop-1.0.3/conf/mapred-site.xml"));
            		init();
                    return null;
                }
            });
			
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	protected static void init() throws IOException, ClassNotFoundException {
		String []args = {"D:/1.txt","hiarchive/4"};
		//String []args = {"hiarchive/output/103"};
		HDFSClient client = new HDFSClient();
		
			//client.addFile(args[0], args[1]);
			//client.readFile(args[0]);
			client.listContent("historical/113");
			//System.out.println(client.ifExists(new Path(args[0])));
			//client.mkdir(args[0]);
			//client.getHostnames();
			//client.addObject(new Text("Twitter","msg"), "test", "text02");
			//client.readObject("test/text01");
		//client.copyMerge("historical/113", "hiarchive/113/1");
		//client.delete("hiarchive");


		System.out.println("Done!");
		
	}
}
