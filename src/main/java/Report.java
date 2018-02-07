import com.marklogic.xcc.*;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccConfigException;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Report {


    private static Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static boolean RUN_FULL_REPORT = true;
    private static String lastProcessedURI = "()";
    private static boolean complete = false;
    private static ExecutorService es = Executors.newFixedThreadPool(Config.THREAD_POOL_SIZE);
    private static ContentSource csSource = null;

    private static Map<String, Integer> documentMap = new ConcurrentHashMap<>();

    private static ResultSequence getBatch(String uri, Session sourceSession) {

        String query = String.format("for $i in cts:uris ( \"%s\", ('frequency-order', 'document', 'limit=1000')) return ($i||'~'||cts:frequency($i))", uri);

        LOG.debug(query);

        Request request = sourceSession.newAdhocQuery(query);
        ResultSequence rs = null;
        try {
            rs = sourceSession.submitRequest(request);
        } catch (RequestException e) {
            e.printStackTrace();
        }
        boolean moreThanOne = (rs.size() > 1);

        if (!moreThanOne) {
            LOG.info(String.format("Down to the last item in the list: %d URI returned", rs.size()));
            complete = true;
            rs.close();
            rs = null;
        }
        return rs;
    }

    public static void main(String[] args) {


        try {
            csSource = ContentSourceFactory.newContentSource(URI.create(Config.INPUT_XCC_URI));
            Session sourceSession = csSource.newSession();

            while (!complete) {
                LOG.debug("Itemlist not complete - more URIs still to process.");
                processResultSequence(documentMap, getBatch(lastProcessedURI, sourceSession));
            }

            // Stop the thread pool
            es.shutdown();
            // Drain the queue
            while (!es.isTerminated()) {
                try {
                    es.awaitTermination(72, TimeUnit.HOURS);
                } catch (InterruptedException e) {
                    LOG.error("Exception caught: ", e);
                }
            }

            sourceSession.close();


            if (RUN_FULL_REPORT) {
                LOG.debug("About to run the report...");
                runFinalReport(documentMap);
            }

        } catch (XccConfigException | RequestException e) {
            LOG.error("Exception caught: ", e);
        }
    }

    private static void runFinalReport(Map<String, Integer> documentMap) {
        LOG.info("Generating report ...");
        LOG.info("Map contains " + documentMap.size() + " entries");
    }

    private static void processResultSequence(Map<String, Integer> documentMap, ResultSequence rs) throws RequestException {
        if (rs != null) {
            if (rs.size() <= 1) {
                LOG.debug("Only one item returned - is this the end of the run?");
                complete = true;
            }

            LOG.debug(String.format("Starting with a batch of %d documents", rs.size()));

            Iterator<ResultItem> resultItemIterator = rs.iterator();
            while (resultItemIterator.hasNext()) {

                ResultItem i = resultItemIterator.next();
                String item = i.asString();
                String uri = item.substring(0,item.lastIndexOf("~"));
                int frequency = Integer.parseInt(item.substring(item.lastIndexOf("~")+1));
                LOG.info(item);
                // TODO - this ASSUMES we are only looking for doc fragments - we're only searching for document URI instances this time.
                if(frequency > 1 && ! documentMap.containsKey(uri)) {
                    LOG.debug("DUP URI FOUND: " + uri + frequency);
                    es.execute(new FragmentChecker(uri, frequency));
                }

                lastProcessedURI = i.asString();
            }

            LOG.debug(String.format("Last URI in batch of %d URI(s): %s%s%s", rs.size(), Config.ANSI_BLUE, lastProcessedURI, Config.ANSI_RESET));
            rs.close();
        }
    }


    public static class FragmentChecker implements Runnable {

        String uri;
        int frequency;
        String fnDocQ;
        String fnDeleteQ;

        FragmentChecker(String s, int f) {
            LOG.debug(String.format("Working on: %s", s));
            this.uri = s;
            this.frequency = f;
            this.fnDocQ = "fn:doc(\""+uri+"\")";
            this.fnDeleteQ = "xdmp:document-delete(\""+uri+"\")";
        }

        public void run(){
            documentMap.put(uri, frequency);

            Session s = csSource.newSession();


            //LOG.info(q);
            Request request = s.newAdhocQuery(fnDocQ);
            ResultSequence rs = null;
            try {
                rs = s.submitRequest(request);
            } catch (RequestException e) {
                // make sure we get an DBDUPURI exception from the stacktrace!
                StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw));
                String exceptionAsString = sw.toString().split(System.lineSeparator(), 2)[0];
                if(exceptionAsString.contains("XDMP-DBDUPURI")){
                    dealwithDoc(uri, s, exceptionAsString);
                }
            }
        }

        public void dealwithDoc(String uri, Session s, String exc){
            LOG.debug("Duplicate URI needs to be managed:" + uri);
            LOG.debug(exc);
            String[] bits = exc.split("--")[1].split(" ");

            if(bits.length == 11){
               String forestA = bits[6];
               String forestB = bits[8];
               String ts = bits[10];
               LOG.info("Document "+ uri + " found in " + forestA +" and " + forestB + " at timestamp "+ ts);

               String evalA = "xdmp:eval('"+fnDocQ+"', (), <options xmlns=\"xdmp:eval\"><database>{xdmp:forest('"+forestA+"')}</database></options>)";
               String evalB = "xdmp:eval('"+fnDocQ+"', (), <options xmlns=\"xdmp:eval\"><database>{xdmp:forest('"+forestB+"')}</database></options>)";
               String md5A = null;
               String md5B = null;
               String deletionCandidate = null;

               LOG.debug(evalA);
               Request request = s.newAdhocQuery(evalA);
                try {
                    ResultSequence rs = s.submitRequest(request);
                    md5A = DigestUtils.md5Hex( rs.asString() );
                    //LOG.info("MD5 for document in "+forestA +" "+md5A);
                } catch (RequestException e) {
                    e.printStackTrace();
                }

                request = s.newAdhocQuery(evalB);
                try {
                    ResultSequence rs = s.submitRequest(request);
                    deletionCandidate = rs.asString();
                    md5B = DigestUtils.md5Hex( deletionCandidate );
                    //LOG.info("MD5 for document in "+forestB +" "+md5B);
                } catch (RequestException e) {
                    e.printStackTrace();
                }

                if (md5A!=null) {
                    if (md5A.equals(md5B)) {
                        LOG.info("Safe to delete: "+ uri +" from "+ forestB + " as MD5s match "+ md5A+"/"+md5B);
                        // save the doc to disk
                        boolean backedup = false;
                        try {
                            Files.write(Paths.get("src/main/resources/backup"+uri), deletionCandidate.getBytes());
                            backedup = true;
                        } catch (IOException e) {
                            LOG.error("Error backing file up "+uri);
                        }
                        if(backedup) {
                            // do the delete
                            String deleteEval = "xdmp:eval('"+fnDeleteQ+"', (), <options xmlns=\"xdmp:eval\"><database>{xdmp:forest('"+forestB+"')}</database></options>)";
                            LOG.info(deleteEval);
                            Request deleteRequest = s.newAdhocQuery(deleteEval);
                            try {
                                ResultSequence rs2 = s.submitRequest(deleteRequest);
                                LOG.info("Deleted URI: "+uri+rs2.asString());
                            } catch (RequestException e) {
                                LOG.error("Problem deleting document with URI: "+uri+" from forest "+forestB);
                            }
                        } else {
                            LOG.warn("Not fixed "+ uri + " please see error log for details");
                        }
                    }
                } else {
                    LOG.warn("NOT DELETING: "+uri + " - NO MD5 MATCH! "+ md5A+"/"+md5B);
                }

            } else {
                LOG.warn("*** ALERT --- CAN WE FIND OUT MORE ABOUT THIS DOC BEFORE WE DO ANYTHING! *** : "+uri);
            }

        }
    }

}
