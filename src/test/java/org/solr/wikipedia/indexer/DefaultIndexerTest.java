package org.solr.wikipedia.indexer;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.solr.wikipedia.handler.DefaultPageHandler;
import org.solr.wikipedia.iterator.SolrInputDocPageIterator;
import org.solr.wikipedia.iterator.WikiMediaIterator;
import org.solr.wikipedia.model.Page;

import javax.xml.stream.XMLStreamException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import org.solr.wikipedia.util.EmbeddedSolrServerFactory;

/**
 * @author bryanbende
 */
public class DefaultIndexerTest {

    private SolrClient solrClient;

    private DefaultIndexer defaultIndexer;

    @Before
    public void setup() {
        this.solrClient = EmbeddedSolrServerFactory.create("wikipediaCollection");
        this.defaultIndexer = new DefaultIndexer(solrClient);
    }

    @After
    public void after() throws IOException {
        this.solrClient.close();
    }

    @Test
    public void testIndexPages() throws IOException, XMLStreamException, SolrServerException {
        String testWikiXmlFile = "src/test/resources/test-wiki-data.xml";

        try (FileReader reader = new FileReader(testWikiXmlFile)) {
            Iterator<Page> pageIter = new WikiMediaIterator<>(
                    reader, new DefaultPageHandler());

            Iterator<SolrInputDocument> docIter =
                    new SolrInputDocPageIterator(pageIter);

            defaultIndexer.index(docIter);
            solrClient.commit();
        }

        SolrQuery solrQuery = new SolrQuery("*:*");
        QueryResponse response = solrClient.query(solrQuery);
        Assert.assertEquals(3, response.getResults().size());
    }

    @Test
    public void testIndexPages2019() throws IOException, XMLStreamException, SolrServerException {
        String testWikiXmlFile = "src/test/resources/test-wiki-data_2019.xml";

        try (FileReader reader = new FileReader(testWikiXmlFile)) {
            Iterator<Page> pageIter = new WikiMediaIterator<>(
                reader, new DefaultPageHandler());

            Iterator<SolrInputDocument> docIter =
                new SolrInputDocPageIterator(pageIter);

            defaultIndexer.index(docIter);
            solrClient.commit();
        }

        SolrQuery solrQuery = new SolrQuery("*:*");
        QueryResponse response = solrClient.query(solrQuery);
        Assert.assertEquals(3, response.getResults().size());
    }

}
