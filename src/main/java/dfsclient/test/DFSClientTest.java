package dfsclient.test;

import java.io.BufferedOutputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class DFSClientTest {

  private static Logger log = Logger.getLogger(DFSClientTest.class);
  /**
   * @param args
   */
  public static void main(String[] args) {

    try {

      Configuration config = new Configuration();

      FileSystem fs = FileSystem.get(config);

      int i = 0;

      OutputStream os = new BufferedOutputStream(fs.create(new Path(args[0])));

      while (i < Integer.parseInt(args[1])) {

        System.out.println(" waiting before write test " + i);

        log.trace(" waiting before write test " + i);
        log.debug(" waiting before write test " + i);
        log.info(" waiting before write test " + i);

        // synchronized (os) {
        // os.wait(70000);
        // }

        os.write("test".getBytes(), 0, "test".getBytes().length);

        System.out.println("wrote test " + i);
        //
        // synchronized (os) {
        // os.wait(70000);
        // }
        i++;

      }

      os.flush();
      // is.close();
      // fs.close();
      fs.close();

      os.close();

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
