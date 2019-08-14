package org.solr.wikipedia.util;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;

import java.io.File;

/**
 * Helper to create EmbeddedSolrClient instances for testing.
 *
 * @author bryanbende
 */
public class EmbeddedSolrServerFactory {

    public static final String DEFAULT_SOLR_HOME = "src/main/resources/solr";

    public static final String DEFAULT_CORE_HOME = "src/main/resources/";

    public static final String DEFAULT_DATA_DIR = "/tmp";

    /**
     * Use the defaults to create the core.
     *
     * @param coreName
     * @return
     */
    public static SolrClient create(String coreName) {
        return create(DEFAULT_SOLR_HOME, DEFAULT_CORE_HOME,
                coreName, DEFAULT_DATA_DIR);
    }

    /**
     *
     * @param solrHome
     *              path to directory where solr.xml lives
     *
     * @param coreName
     *              the name of the core to load
     * @param dataDir
     *              the data dir for the core
     *
     * @return an EmbeddedSolrClient for the given core
     */
    public static SolrClient create(String solrHome, String coreHome, String coreName, String dataDir) {
        File coreDataDir = new File(dataDir + "/" + coreName);
        if (coreDataDir.exists()) {
            coreDataDir.delete();
        }


        CoreContainer coreContainer = new CoreContainer(solrHome);
        coreContainer.load();

        Map<String, String> props = new HashMap<>();
        props.put("dataDir", dataDir + "/" + coreName);

        SolrCore solrCore = coreContainer.create(coreName, Paths.get(solrHome), props, false);

        return new EmbeddedSolrServer(solrCore);
    }
}
