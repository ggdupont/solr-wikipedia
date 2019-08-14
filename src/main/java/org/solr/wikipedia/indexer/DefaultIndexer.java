package org.solr.wikipedia.indexer;

import java.util.zip.GZIPInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.lang3.Validate;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.solr.wikipedia.handler.DefaultPageHandler;
import org.solr.wikipedia.iterator.SolrInputDocPageIterator;
import org.solr.wikipedia.iterator.WikiMediaIterator;
import org.solr.wikipedia.model.Page;

import javax.xml.stream.XMLStreamException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Indexes Pages in Solr.
 *
 * @author bryanbende
 */
public class DefaultIndexer {

    static final int DEFAULT_BATCH_SIZE = 20;

    private final int batchSize;
    private final SolrClient solrClient;

    /**
     *
     * @param solrClient
     */
    public DefaultIndexer(SolrClient solrClient) {
        this(solrClient, DEFAULT_BATCH_SIZE);
    }

    /**
     *
     * @param solrClient
     * @param batchSize
     */
    public DefaultIndexer(SolrClient solrClient, int batchSize) {
        this.solrClient = solrClient;
        this.batchSize = batchSize;
        Validate.notNull(this.solrClient);
        if (this.batchSize <= 0) {
            throw new IllegalStateException("Batch size must be > 0");
        }
    }

    /**
     * Iterates over docs adding each SolrInputDocument to the given SolrServer in batches.
     */
    public void index(Iterator<SolrInputDocument> docs) throws IOException, SolrServerException {
        if (docs == null) {
            return;
        }

        long count = 0;
        Collection<SolrInputDocument> solrDocs = new ArrayList<>();
        while (docs.hasNext()) {
            SolrInputDocument doc = docs.next();

            for (String fieldName : doc.getFieldNames()) {
                SolrInputField field = doc.getField(fieldName);
                if (field != null && field.toString().length() > 32766) {
                    System.err.println("Field " + fieldName + " truncated at 32000 characters.");
                    field.setValue(field.toString().substring(0, 32000));
                }
            }

            solrDocs.add(doc);

            if (solrDocs.size() >= this.batchSize) {
                solrClient.add(solrDocs);
                count += solrDocs.size();
                System.out.println("reached batch size, total count = " + count);
                solrDocs.clear();
            }
        }

        if (solrDocs.size() > 0) {
            solrClient.add(solrDocs);
            solrDocs.clear();
        }
    }


    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: DefaultIndexer <SOLR_URL> <WIKIPEDIA_DUMP_FILE> " +
                "(<BATCH_SIZE>)");
            System.exit(0);
        }

        String solrUrl = args[0];
        String wikimediaDumpFile = args[1];

        Validate.notEmpty(solrUrl);
        Validate.notEmpty(wikimediaDumpFile);

        // attempt to parse a provided batch size
        Integer batchSize = null;
        if (args.length == 3) {
            try {
                batchSize = Integer.valueOf(args[2]);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // TODO clean-up duplicate code
        if (wikimediaDumpFile.endsWith(".bz2")) {
            try (FileInputStream fileIn = new FileInputStream(wikimediaDumpFile);
                BZip2CompressorInputStream bzipIn = new BZip2CompressorInputStream(fileIn);
                InputStreamReader reader = new InputStreamReader(bzipIn)) {

                Iterator<Page> pageIter = new WikiMediaIterator<>(
                    reader, new DefaultPageHandler());

                Iterator<SolrInputDocument> docIter =
                    new SolrInputDocPageIterator(pageIter);

                SolrClient solrClient = new HttpSolrClient.Builder(solrUrl)
                    .withConnectionTimeout(10000)
                    .withSocketTimeout(60000)
                    .build();
                ;

                DefaultIndexer defaultIndexer = (batchSize != null ?
                    new DefaultIndexer(solrClient, batchSize) :
                    new DefaultIndexer(solrClient));

                long startTime = System.currentTimeMillis();

                defaultIndexer.index(docIter);

                System.out.println("Indexing finished at " + new Date());
                System.out.println("Took " + (System.currentTimeMillis() - startTime) + " ms");

            } catch (IOException e) {
                e.printStackTrace();
            } catch (XMLStreamException e) {
                e.printStackTrace();
            } catch (SolrServerException e) {
                e.printStackTrace();
            }
        }

        if (wikimediaDumpFile.endsWith(".gz")) {
            try (FileInputStream fileIn = new FileInputStream(wikimediaDumpFile);
                GZIPInputStream bzipIn = new GZIPInputStream(fileIn);
                InputStreamReader reader = new InputStreamReader(bzipIn)) {

                Iterator<Page> pageIter = new WikiMediaIterator<>(
                    reader, new DefaultPageHandler());

                Iterator<SolrInputDocument> docIter =
                    new SolrInputDocPageIterator(pageIter);

                SolrClient solrClient = new HttpSolrClient.Builder(solrUrl).
                    withConnectionTimeout(10000)
                    .withSocketTimeout(60000)
                    .build();

                DefaultIndexer defaultIndexer = (batchSize != null ?
                    new DefaultIndexer(solrClient, batchSize) :
                    new DefaultIndexer(solrClient));

                long startTime = System.currentTimeMillis();

                defaultIndexer.index(docIter);

                System.out.println("Indexing finished at " + new Date());
                System.out.println("Took " + (System.currentTimeMillis() - startTime) + " ms");

            } catch (IOException e) {
                e.printStackTrace();
            } catch (XMLStreamException e) {
                e.printStackTrace();
            } catch (SolrServerException e) {
                e.printStackTrace();
            }
        }

        if (wikimediaDumpFile.endsWith(".xml")) {
            try (FileInputStream fileIn = new FileInputStream(wikimediaDumpFile);
                InputStreamReader reader = new InputStreamReader(fileIn)) {

                Iterator<Page> pageIter = new WikiMediaIterator<>(
                    reader, new DefaultPageHandler());

                Iterator<SolrInputDocument> docIter =
                    new SolrInputDocPageIterator(pageIter);

                SolrClient solrClient = new HttpSolrClient.Builder(solrUrl)
                    .withConnectionTimeout(10000)
                    .withSocketTimeout(60000)
                    .build();

                DefaultIndexer defaultIndexer = (batchSize != null ?
                    new DefaultIndexer(solrClient, batchSize) :
                    new DefaultIndexer(solrClient));

                long startTime = System.currentTimeMillis();

                defaultIndexer.index(docIter);

                System.out.println("Indexing finished at " + new Date());
                System.out.println("Took " + (System.currentTimeMillis() - startTime) + " ms");

            } catch (IOException e) {
                e.printStackTrace();
            } catch (XMLStreamException e) {
                e.printStackTrace();
            } catch (SolrServerException e) {
                e.printStackTrace();
            }
        }
    }

}
