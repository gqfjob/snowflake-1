package com.twitter.service.snowflake.server;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;

import com.twitter.service.snowflake.IdWorker;
import com.twitter.service.snowflake.Snowflake;
import com.twitter.service.snowflake.Snowflake.Iface;
import com.twitter.service.snowflake.Snowflake.Processor;

public class SnowflakeServer {

    private static final Logger logger = Logger
            .getLogger(SnowflakeServer.class);

    public void serv(long workerId, long datacenterId, String host, int port,
            int selectThs, int workerThs) throws Exception {
        IdWorker idWorker = new IdWorker(workerId, datacenterId);
        Processor<Iface> processor = new Snowflake.Processor<Iface>(idWorker);
        TNonblockingServerTransport transport = new TNonblockingServerSocket(
                new InetSocketAddress(host, port));
        TThreadedSelectorServer server = new TThreadedSelectorServer(
                new TThreadedSelectorServer.Args(transport)
                        .processor(processor).selectorThreads(selectThs)
                        .workerThreads(workerThs)
                        .executorService(Executors.newCachedThreadPool()));
        logger.info("Snowflake serv on " + host + ":" + port
                + ", args : workerId(" + workerId + "), datacenterId("
                + datacenterId + "), selectThs(" + selectThs + "), workerThs(" + workerThs + ")");
        server.serve();
    }

    public static void main(String[] args) throws Exception {
        String usage = "Usage:java -jar snowflake*.jar -wid workerId(<31) -dcid datacenterId(<31) -h host -p port -sths selectThreads(>0) -wths workerThreads(>0)";
        if (args.length < 12) {
            System.out.println(usage);
            System.exit(0);
        }
        long workerId = -1;
        long datacenterId = -1;
        String host = "";
        int port = -1;
        int selectThs = 0;
        int workerThs = 0;
        for (int i = 0; i < args.length; i++) {
            if (i % 2 == 1) {
                continue;
            }
            if (i >= 12) {
                break;
            }
            String a = args[i];
            if (a.equals("-wid")) {
                workerId = Long.valueOf(args[i + 1]);
            }
            if (a.equals("-dcid")) {
                datacenterId = Long.valueOf(args[i + 1]);
            }
            if (a.equals("-h")) {
                host = args[i + 1];
            }
            if (a.equals("-p")) {
                port = Integer.valueOf(args[i + 1]);
            }
            if (a.equals("-sths")) {
                selectThs = Integer.valueOf(args[i + 1]);
            }
            if (a.equals("-wths")) {
                workerThs = Integer.valueOf(args[i + 1]);
            }
        }
        if (workerId == -1 || workerId > 31 || datacenterId == -1
                || datacenterId > 31 || host.equals("") || port == -1
                || selectThs < 1 || workerThs < 1) {
            System.out.println(usage);
            System.exit(0);
        }
        new SnowflakeServer().serv(workerId, datacenterId, host, port,
                selectThs, workerThs);
    }

}
