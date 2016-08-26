import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by ssoldatov on 5/2/16.
 */
public class PerfTest {


    private static final Logger logger = LoggerFactory.getLogger(PerfTest.class);

    static volatile int num =0;
    static int batch = 500;

    static Connection conn;
    static volatile long numberRows = 0;
    static boolean autoCommit = false;
    public static void main(String[] args) throws Exception {
        if(args.length < 3) {
            logger.info("Usage: java -jar ... <zk quorum> <number of threads> <number of rows per" +
                    " thread> <number of buckets (optional)>");
            return;
        }
        final String uri = args[0];
        final int threadsNum = Integer.parseInt(args[1]);
        final int numRows = Integer.parseInt(args[2]);
        int sbt = 0;
        if (args.length > 3) {
            sbt = Integer.parseInt(args[3]);
        }
        doTest(uri, threadsNum, numRows, sbt);
    }
    public static void doTest(final String uri, final int threadNum, final int numRows, final int sb) throws SQLException, InterruptedException {
        conn = DriverManager.getConnection("jdbc:phoenix:" + uri);
        logger.info("Creating test table...");
        String drl = "DROP TABLE IF EXISTS test_table";
        conn.createStatement().execute(drl);
        String ddl = "CREATE TABLE IF NOT EXISTS test_table (id INTEGER not null , id2 integer not null, id3 integer not null, id4 integer not null, s1 VARCHAR, s2 varchar, s3 varchar constraint pk primary key (id, id2, id3, id4))";
        if (sb > 0) {
            ddl = ddl + " SALT_BUCKETS = " + sb;
        }
        logger.info("Table created as : " + ddl);
        conn.createStatement().execute(ddl);
        logger.info("Starting " + threadNum + " threads with " + numRows + " rows each");
	Thread.sleep(10000);

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < threadNum; i++) {
            new Thread() {
                @Override
                public void run() {
                    num++;
                    Connection c1 = null;
                    try {
                        c1 = DriverManager.getConnection("jdbc:phoenix:" + uri);
                        c1.setAutoCommit(autoCommit);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                    ;
                    Random r = new Random();
                    int i = r.nextInt(20);
                    String pss = "Upsert into test_table values (?,?,?,?,?,?)";
                    try {
                        PreparedStatement ps = c1.prepareStatement(pss);
                        for (int j = 0; j < numRows; j++) {
                            ps.setInt(1, i);
                            ps.setInt(2, j);
                            ps.setInt(3, r.nextInt(100));
                            ps.setInt(4, r.nextInt(100));
                            ps.setString(5, UUID.randomUUID().toString());
                            ps.setString(6, UUID.randomUUID().toString());
                            ps.execute();
                            if(c1.getAutoCommit()) {
                                numberRows++;
                            } else {
                                if (j % batch == 0) {
                                    c1.commit();
                                    numberRows += batch;
                                }
                            }
                        }
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }

                    try {
                        c1.commit();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                    num--;
                }
            }.start();
        }
        Thread.sleep(50);
        long prevNum = 0;
        long timeMs = 0;
        while (num != 0) {
            Thread.sleep(1000);
            timeMs += 1000;
            if (numberRows != 0) {
                long currentRows = numberRows - prevNum;
                prevNum = numberRows;
                logger.info("Wrote " + currentRows);
            }
        }
        long endTime = System.currentTimeMillis();

        logger.info("Test took " + (endTime - startTime) + " milliseconds");
        long perf = numRows /((endTime-startTime)/1000) * threadNum;
        logger.info(perf + " records / sec");
        logger.info("dropping table");
        drl = "DROP TABLE IF EXISTS test_table";
        //conn.createStatement().execute(drl);
        conn.close();
    }
}
