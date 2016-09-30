package info.rires.index;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.zip.GZIPInputStream;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import info.rires.document.TrecTextDocIterator;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;


public final class RiseBuildIndex {
    private static final Logger LOG = LogManager.getLogger(RiseBuildIndex.class);
    private final Path indexPath;
    private final Path docDir;
    private final String docType;
    private final String stemmer;
    private final boolean removeStopwords;

    private final class IndexerThread extends Thread {

        final private Path inputWarcFile;

        final private IndexWriter writer;

        public IndexerThread(IndexWriter writer, Path inputWarcFile) throws IOException {
            this.writer = writer;
            this.inputWarcFile = inputWarcFile;
            setName(inputWarcFile.getFileName().toString());
        }

        private int indexWarcRecord(WarcRecord warcRecord) throws IOException {
            // see if it's a response record
            if (!RESPONSE.equals(warcRecord.type()))
                return 0;

            String id = warcRecord.id();

            org.jsoup.nodes.Document jDoc;
            try {
                jDoc = Jsoup.parse(warcRecord.content());
            } catch (java.lang.IllegalArgumentException iae) {
                LOG.error("Parsing document with JSoup failed, skipping document : " + id, iae);
                System.err.println(id);
                return 1;
            }

            String contents = jDoc.text();
            // don't index empty documents but count them
            if (contents.trim().length() == 0) {
                System.err.println(id);
                return 1;
            }

            // make a new, empty document
            Document document = new Document();

            // document id
            document.add(new StringField(FIELD_ID, id, Field.Store.YES));

            FieldType fieldType = new FieldType();

            // Are we storing document vectors?
            if (docVectors) {
                fieldType.setStored(false);
                fieldType.setStoreTermVectors(true);
                fieldType.setStoreTermVectorPositions(true);
            }

            // Are we building a "positional" or "count" index?
            if (positions) {
                fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            } else {
                fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
            }

            document.add(new Field(FIELD_BODY, contents, fieldType));

            writer.addDocument(document);
            return 1;

        }

        private int indexClueWeb12WarcFile() throws IOException {

            int i = 0;

            try (DataInputStream inStream = new DataInputStream(new GZIPInputStream(Files.newInputStream(inputWarcFile, StandardOpenOption.READ)))) {
                // iterate through our stream
                ClueWeb12WarcRecord wDoc;
                while ((wDoc = ClueWeb12WarcRecord.readNextWarcRecord(inStream, ClueWeb12WarcRecord.WARC_VERSION)) != null) {
                    i += indexWarcRecord(wDoc);
                }
            }
            return i;
        }

        private int indexClueWeb09WarcFile() throws IOException {

            int i = 0;

            try (DataInputStream inStream = new DataInputStream(new GZIPInputStream(Files.newInputStream(inputWarcFile, StandardOpenOption.READ)))) {
                // iterate through our stream
                ClueWeb09WarcRecord wDoc;
                while ((wDoc = ClueWeb09WarcRecord.readNextWarcRecord(inStream, ClueWeb09WarcRecord.WARC_VERSION)) != null) {
                    i += indexWarcRecord(wDoc);
                }
            }
            return i;
        }

        private int indexGov2File() throws IOException {
            long i = 0;
            StringBuilder builder = new StringBuilder();
            boolean found = false;
            try (
                InputStream stream = new GZIPInputStream(Files.newInputStream(inputWarcFile, StandardOpenOption.READ),
                        Gov2Record.BUFFER_SIZE);
                BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {

                TrecTextDocIterator docs = new TrecTextDocIterator(reader);
                Document doc;
                while (docs.hasNext()) {
                    doc = docs.next();
                    if (doc != null && doc.getField("contents") != null) {
                        writer.addDocument(doc);
                    }
                }

                for (; ; ) {
                    String line = reader.readLine();
                    if (line == null)
                        break;

                    line = line.trim();

                    if (line.startsWith(Gov2Record.DOC)) {
                        found = true;
                        continue;
                    }

                    if (line.startsWith(Gov2Record.TERMINATING_DOC)) {
                        found = false;
                        WarcRecord gov2 = Gov2Record.parseGov2Record(builder);
                        i += indexWarcRecord(gov2);
                        builder.setLength(0);
                    }

                    if (found)
                        builder.append(line).append(" ");
                }
            }

            return i;
        }

        @Override
        public void run() {
            {
                try {
                    if ("clueweb09".equals(docType)) {
                        int addCount = indexClueWeb09WarcFile();
                        System.out.println("*./" + inputWarcFile.getParent().getFileName().toString()
                                + File.separator + inputWarcFile.getFileName().toString() + "  " + addCount);
                    } else if ("clueweb12".equals(docType)) {
                        int addCount = indexClueWeb12WarcFile();
                        System.out.println("./" + inputWarcFile.getParent().getFileName().toString()
                                + File.separator + inputWarcFile.getFileName().toString() + "\t" + addCount);
                    } else if ("trecweb".equals(docType)) {
                        int addCount = indexGov2File();
                        System.out.println("./" + inputWarcFile.getParent().getFileName().toString()
                                + File.separator + inputWarcFile.getFileName().toString() + "\t" + addCount);
                    } else if ("trectext".equals(docType)) {
                        int addCount = indexGov2File();
                        System.out.println("./" + inputWarcFile.getParent().getFileName().toString()
                                + File.separator + inputWarcFile.getFileName().toString() + "\t" + addCount);
                    }
                } catch (IOException ioe) {
                    LOG.error(Thread.currentThread().getName() + ": ERROR: unexpected IOException:", ioe);
                }
            }
        }
    }

    static Deque<Path> discoverWarcFiles(Path p) {

        final Deque<Path> stack = new ArrayDeque<>();

        FileVisitor<Path> fv = new SimpleFileVisitor<Path>() {

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {

                Path name = file.getFileName();
                if (name != null) {
                    stack.add(file);
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                if ("OtherData".equals(dir.getFileName().toString())) {
                    LOG.info("Skipping: " + dir);
                    return FileVisitResult.SKIP_SUBTREE;
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException ioe) {
                LOG.error("Visiting failed for " + file.toString(), ioe);
                return FileVisitResult.SKIP_SUBTREE;
            }
        };

        try {
            Files.walkFileTree(p, fv);
        } catch (IOException e) {
            LOG.error("IOException during file visiting", e);
        }
        return stack;
    }

    public int indexWithThreads(int numThreads) throws IOException, InterruptedException {

        LOG.info("Indexing with " + numThreads + " threads to directory '" + indexPath.toAbsolutePath() + "'...");

        final Directory dir = FSDirectory.open(this.indexPath);

        final IndexWriterConfig iwc;
        if (this.removeStopwords) {
            iwc = new IndexWriterConfig(new EnglishAnalyzer());
        } else {
            iwc = new IndexWriterConfig(new EnglishAnalyzer(CharArraySet.EMPTY_SET));
        }
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        iwc.setRAMBufferSizeMB(512);
        iwc.setUseCompoundFile(false);
        iwc.setMergeScheduler(new ConcurrentMergeScheduler());

        final IndexWriter writer = new IndexWriter(dir, iwc);

        final ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThreads);
        final Deque<Path> allFiles = discoverWarcFiles(docDir);

        long totalFiles = allFiles.size();
        LOG.info(totalFiles + " many files found under the docs path : " + docDir.toString());

        for (int i = 0; i < 2000; i++) {
            if (!allFiles.isEmpty())
                executor.execute(new IndexerThread(writer, allFiles.removeFirst()));
            else {
                if (!executor.isShutdown()) {
                    Thread.sleep(30000);
                    executor.shutdown();
                }
                break;
            }
        }

        long first = 0;
        //add some delay to let some threads spawn by scheduler
        Thread.sleep(30000);

        try {
            // Wait for existing tasks to terminate
            while (!executor.awaitTermination(1, TimeUnit.MINUTES)) {

                final long completedTaskCount = executor.getCompletedTaskCount();

                LOG.info(String.format("%.2f percentage completed", (double) completedTaskCount / totalFiles * 100.0d));

                if (!allFiles.isEmpty())
                    for (long i = first; i < completedTaskCount; i++) {
                        if (!allFiles.isEmpty())
                            executor.execute(new IndexerThread(writer, allFiles.removeFirst()));
                        else {
                            if (!executor.isShutdown())
                                executor.shutdown();
                        }
                    }

                first = completedTaskCount;
                Thread.sleep(1000);
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            executor.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }

        if (totalFiles != executor.getCompletedTaskCount())
            throw new RuntimeException("totalWarcFiles = " + totalFiles + " is not equal to completedTaskCount =  " + executor.getCompletedTaskCount());


        int numIndexed = writer.maxDoc();

        try {
            writer.commit();
        } finally {
            writer.close();
        }

        return numIndexed;
    }

    public RiseBuildIndex(String docsPath, String indexPath, String docType, String stemmer, boolean removeStopwords) throws IOException {
        this.indexPath = Paths.get(indexPath);
        if (!Files.exists(this.indexPath)) {
            Files.createDirectories(this.indexPath);
        }

        docDir = Paths.get(docsPath);
        if (!Files.exists(docDir) || !Files.isReadable(docDir) || !Files.isDirectory(docDir)) {
            System.out.println("Document directory '" + docDir.toString() + "' does not exist or is not readable, please check the path");
            System.exit(1);
        }

        this.docType = docType;
        this.stemmer = stemmer;
        this.removeStopwords = removeStopwords;
    }

    public static void main(String[] args) {
        String indexDir = "/Users/yizhang/Index";
        String dataDir = "/Users/yizhang/Data";
        Indexer indexer;
        Searcher searcher;

        /*
        try {
            indexer = new Indexer(indexDir);
            int numIndexed;
            long startTime = System.currentTimeMillis();
            numIndexed = indexer.createIndex(dataDir, new TextFileFilter());
            long endTime = System.currentTimeMillis();
            indexer.close();
            System.out.println(numIndexed+" File indexed, time taken: "
                    +(endTime-startTime)+" ms");
        } catch (IOException e) {
            e.printStackTrace();
        }*/

        /*
        try {
            String q = "fox";
            searcher = new Searcher(indexDir);
            long startTime = System.currentTimeMillis();
            TopDocs hits = searcher.search(q);
            long endTime = System.currentTimeMillis();
            System.out.println(searcher.queryParser.parse("tests computers").toString());

            System.out.println(hits.totalHits +
                    " documents found. Time :" + (endTime - startTime) +" ms");
            for(ScoreDoc scoreDoc : hits.scoreDocs) {
                Document doc = searcher.getDocument(scoreDoc);
                System.out.println("File: "+ doc.get(LuceneConstants.FILE_PATH) + " "+scoreDoc.score);
                System.out.println("Explain: "+ searcher.indexSearcher.explain(searcher.queryParser.parse(q), scoreDoc.doc));
            }
            searcher.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }*/

        // create the command line parser
        CommandLineParser parser = new DefaultParser();

        // create the Options
        Options options = new Options();
        options.addOption( Option.builder("i")
                .longOpt( "input_path" )
                .desc( "the path to the raw documents" )
                .required(true)
                .hasArg()
                .argName("INPUT_PATH")
                .build() );
        options.addOption( Option.builder("o")
                .longOpt( "output_path" )
                .desc( "the path to the output index" )
                .required(true)
                .hasArg()
                .argName("INDEX")
                .build() );
        options.addOption( Option.builder("f")
                .longOpt( "format" )
                .desc( "the format of the inputs: \ntrectext(for trec1,2,3,6,7,8 robust04)\ntrecweb(for wt2g, gov2)\nclueweb09\nclueweb12" )
                .required(true)
                .hasArg()
                .argName("FORMAT")
                .build() );
        options.addOption( "r", "remdove-stopwords", true, "whether to remove stopwords, default [true]" );
        options.addOption( Option.builder("s")
                .longOpt("stemmer")
                .desc( "type of the stemmer" )
                .required(false)
                .hasArg()
                .argName("STEMMER")
                .build() );
        options.addOption( Option.builder("n")
                .longOpt( "numThreads" )
                .desc( "number of threads (in parallel) used to build the index" )
                .required(false)
                .hasArg()
                .argName("NUM_THREADS")
                .build() );

        // automatically generate the help statement
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "RiseBuildIndex", options );

        try {
            // parse the command line arguments
            CommandLine line = parser.parse( options, args );
        }
        catch( ParseException exp ) {
            System.out.println( "Unexpected exception:" + exp.getMessage() );
        }

        final long start = System.nanoTime();
        /*IndexWebCollection indexer = new IndexWebCollection(indexArgs.input, indexArgs.index, indexArgs.collection);

        indexer.setDocVectors(indexArgs.docvectors);
        indexer.setPositions(indexArgs.positions);
        indexer.setOptimize(indexArgs.optimize);
        indexer.setDocLimit(indexArgs.doclimit);

        LOG.info("Index path: " + indexArgs.index);
        LOG.info("Threads: " + indexArgs.threads);
        LOG.info("Doc limit: " + (indexArgs.doclimit == -1 ? "all docs" : "" + indexArgs.doclimit));

        LOG.info("Indexer: start");

        int numIndexed = indexer.indexWithThreads(indexArgs.threads);
        final long durationMillis = TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        LOG.info("Total " + numIndexed + " documents indexed in " + DurationFormatUtils.formatDuration(durationMillis, "HH:mm:ss"));*/
    }
}
