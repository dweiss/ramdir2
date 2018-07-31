package com.carrotsearch.ramdir2;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.IOUtils;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.carrotsearch.progresso.Progress;
import com.carrotsearch.progresso.RangeTracker;
import com.carrotsearch.progresso.Tasks;
import com.carrotsearch.progresso.Tracker;
import com.carrotsearch.progresso.views.console.ConsoleAware;
import com.google.common.base.Stopwatch;

@Parameters(
    separators = "= ",
    commandNames = RamDirCmd.CMD_NAME,
    commandDescription = "Run RamDirectory checks.") 
public class RamDirCmd extends CommandModule {
  final static String CMD_NAME = "ramdir";

  public static enum DirImpl {
    FS_DIR,
    RAM_DIR,
    RAM_DIR2
  }
  
  @Parameter(
      names = "--threads",
      listConverter = IntStringConverter.class)
  List<Integer> threadCounts;
  {
    threadCounts = IntStream.range(1, Runtime.getRuntime().availableProcessors())
        .filter(v -> v == 1 || v % 2 == 0)
        .mapToObj(v -> (Integer) v)
        .collect(Collectors.toList());
  }

  @Parameter(names = "--time-ms")
  int measurementTimeMillis = 5_000;

  @Parameter(names = "--docs")
  int docs = 500_000;

  @Parameter(names = "--impls")
  List<DirImpl> impls = Arrays.asList(DirImpl.values());

  private final static FieldType FLD_TYPE = ((Supplier<FieldType>) (() -> {
    FieldType ft = new FieldType();
    ft.setTokenized(true);
    ft.setStored(false);
    ft.setOmitNorms(false);
    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS);

    ft.setStoreTermVectors(false);
    ft.setStoreTermVectorPositions(false);
    ft.setStoreTermVectorOffsets(false);
    ft.freeze();
    return ft;
  })).get();
  
  @Override
  public boolean validateArguments() throws IOException {
    return true;
  }

  int uniqTermCount = 10_000;
  int maxTermsPerFld = 50;
  int fieldCount = 3;
  int queryCount = 20_000;
  int clausesPerQuery = 6;

  @Override
  public void execute() throws Exception {
    List<Path> toDelete = new ArrayList<>();
    Path tmp = Files.createTempDirectory("tmpindex");
    toDelete.add(tmp);
    try (Progress progress = new Progress(ConsoleAware.newConsoleProgressView(Arrays.asList()))) {
      // Create a temporary index.
      try (Directory srcDir = FSDirectory.open(tmp)) {
        ArrayList<String> terms = IntStream.range(0, uniqTermCount)
              .mapToObj(i -> String.format(Locale.ROOT, "%06d", i))
              .collect(Collectors.toCollection(ArrayList::new));

        ArrayList<String> fields = IntStream.range(0, fieldCount)
            .mapToObj(i -> "FLD" + i)
            .collect(Collectors.toCollection(ArrayList::new));

        Random rnd = new Random(0xdeadbeef);
        IndexWriterConfig conf = new IndexWriterConfig(new WhitespaceAnalyzer());
        conf.setMergeScheduler(new SerialMergeScheduler());
        try (IndexWriter w = new IndexWriter(srcDir, conf);
             RangeTracker t = progress.newRangeSubtask("Generating index").start(0, docs + 1)) {
          IntStream.range(0, docs)
            .parallel()
            .forEach(new IntConsumer() {
              @Override
              public void accept(int value) {
                try {
                  Random rand = new Random(value);
                  Document doc = new Document();
                  for (String fld : fields) {
                    int tcount = 1 + rand.nextInt(maxTermsPerFld);
                    doc.add(new Field(fld, randomTerms(rand, tcount, terms), FLD_TYPE));
                  }
                  w.addDocument(doc);
                  t.increment();
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }                
              }
            });
          w.forceMerge(1); // to make norms file as large as possible.
          w.commit();
        }
        
        long len = 0;
        for (String file : srcDir.listAll()) {
          len += srcDir.fileLength(file);
        }
        Loggers.CONSOLE.info(String.format(Locale.ROOT, "Index size (MB: %.2f)", (len / (1024.0*1024))));

        ArrayList<Query> queries = new ArrayList<>();
        try (RangeTracker t = progress.newRangeSubtask("Generating queries").start(0, queryCount + 1)) {
          for (int i = 0; i < queryCount; i++) {
            BooleanQuery.Builder qb = new BooleanQuery.Builder();
            for (int k = 0; k < clausesPerQuery; k++) {
              BooleanQuery.Builder cb = new BooleanQuery.Builder();
              String term = randomFrom(rnd, terms);
              for (String fld : fields) {
                cb.add(new TermQuery(new Term(fld, term)), Occur.SHOULD);
              }
              qb.add(cb.build(), Occur.SHOULD);
            }
            qb.setMinimumNumberShouldMatch(1);
            queries.add(qb.build());
            t.increment();
          }
        }

        for (DirImpl impl : impls) {
          Directory dir;
          switch (impl) {
            case RAM_DIR:
              dir = new RAMDirectory();
              break;
            case RAM_DIR2:
              dir = new RamDirectory2();
              break;
            case FS_DIR:
              Path tmpPath = Files.createTempDirectory("fsindex");
              dir = FSDirectory.open(tmpPath);
              toDelete.add(tmpPath);
              break;
            default:
              throw new RuntimeException();
          }

          try (Directory ramDir = copyTo(srcDir, dir);
              DirectoryReader reader = DirectoryReader.open(ramDir)) {
           Loggers.CONSOLE.info("impl=" + impl);
           runTest(progress, threadCounts, queries, reader);
          }
        }

        Loggers.CONSOLE.info("Done.");
      }
    } finally {
      IOUtils.rm(toDelete.toArray(new Path[toDelete.size()]));
    }    
  }

  void runTest(Progress progress, List<Integer> threadCounts, ArrayList<Query> queries, DirectoryReader reader)
      throws InterruptedException {
    final IndexSearcher searcher = new IndexSearcher(reader);
    for (int threadCount : threadCounts) {
      long total;
      long timeMillis;
      try (Tracker t = Tasks.newGenericTask("Querying, threadCount=" + threadCount).start()) {
        AtomicBoolean stop = new AtomicBoolean(false);
        List<Callable<Long>> tasks = IntStream.range(0, threadCount)
          .mapToObj(i -> (Callable<Long>) (() -> {
              try {
                return runQueries(searcher, new Random(i), queries, stop);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
          }))
          .collect(Collectors.toList());
 
        ExecutorService executors = Executors.newFixedThreadPool(threadCount);
        
        Stopwatch sw = Stopwatch.createStarted();
        List<Future<Long>> futures = tasks.stream().map(i -> executors.submit(i)).collect(Collectors.toList());
        Thread.sleep(measurementTimeMillis);
        stop.set(true);
        timeMillis = sw.stop().elapsed(TimeUnit.MILLISECONDS);
        total = futures.stream().mapToLong(f -> {
          try {
            return f.get();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }).sum();

        executors.shutdown();
        executors.awaitTermination(2, TimeUnit.SECONDS);
      }

      double qps = total / (timeMillis / 1000.0d);
      Loggers.CONSOLE.info(String.format("Threads: %s, QPS: %.2f",
          threadCount,
          qps));
    }
  }

  @SuppressWarnings("unused")
  private volatile Object black_hole;

  private Long runQueries(IndexSearcher searcher, Random rnd, ArrayList<Query> queryList, AtomicBoolean stop) throws IOException {
    long total = 0, count = 0;
    while (!stop.get()) {
      Query q = randomFrom(rnd, queryList);
      final TopDocs m = searcher.search(q, 8);
      count += m.scoreDocs.length;
      total++;
    }

    black_hole = count;
    return total;
  }

  private static <T> T randomFrom(Random rnd, ArrayList<T> fields) {
    return fields.get(rnd.nextInt(fields.size()));
  }

  private static String randomTerms(Random rnd, int termsPerDoc, ArrayList<String> terms) {
    StringBuilder b = new StringBuilder();
    for (int i = 0; i < termsPerDoc; i++) {
      if (b.length() > 0) b.append(' ');
      b.append(randomFrom(rnd, terms));
    }
    return b.toString();
  }

  private <T extends Directory> T copyTo(Directory from, T to) throws IOException {
    for (String file : from.listAll()) {
      to.copyFrom(from, file, file, IOContext.DEFAULT);
    }
    return to;
  }

  public static void main(String[] args) throws Exception {
    ArrayList<String> c = new ArrayList<>(Arrays.asList(args));
    c.add(0, RamDirCmd.CMD_NAME);
    Main.main(c.toArray(new String[c.size()]));
  }
}
