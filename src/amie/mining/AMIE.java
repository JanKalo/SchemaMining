/**
 * @author lgalarra
 * @date Aug 8, 2012 AMIE Version 0.1
 */
package amie.mining;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import amie.mining.assistant.experimental.ExistentialRulesHeadVariablesMiningAssistant;
import amie.mining.assistant.experimental.TypedDefaultMiningAssistant;
import amie.mining.assistant.schemamining.SchemaAttributeMiningAssistant;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import amie.data.KB;
import amie.mining.assistant.DefaultMiningAssistant;
import amie.mining.assistant.MiningAssistant;
import amie.mining.assistant.RelationSignatureDefaultMiningAssistant;
import amie.rules.Metric;
import amie.rules.Rule;
import javatools.administrative.Announce;
import javatools.datatypes.ByteString;
import javatools.datatypes.MultiMap;
import javatools.parsers.NumberFormatter;
import sun.awt.X11.XConstants;

/**
 * Main class that implements the AMIE algorithm for rule mining 
 * on ontologies. The ontology must be provided as a list of TSV files
 * where each line has the format SUBJECT&lt;TAB&gt;RELATION&lt;TAB&gt;OBJECT.
 * 
 * @author lgalarra
 *
 */
public class AMIE {


    /**
     * Complete Knowledge Base for computing the Confidence of the generated schemas.
     *
     * This knowledge base is static and shared among all Mining Assistants
     */
    public static KB completeKB;

    /**
     * rdfs:type that should be mined
     */

    private static String type = "http://dbpedia.org/ontology/Person";


    /**
     * Relative support compared to number of entities in the provided type class
     */

    private static double supportPercentage = 0.1;

    /**
     * Minimum schema rule confidence for output
     */


    public static double minConfidence = 0.5;
    /**
     * Cluster Mode
     */

    private static boolean CLUSTER_MODE = false;
    /**
     * Default standard confidence threshold
     */
    private static final double DEFAULT_STD_CONFIDENCE = 0.1;
	
    /**
     * Default PCA confidence threshold
     */
    private static final double DEFAULT_PCA_CONFIDENCE = 0.1;

    /** 
     * Default Head coverage threshold
     *
     */
    private static final double DEFAULT_HEAD_COVERAGE = 0.05;

    /**
     * The default minimum size for a relation to be used as a head relation
     */
    private static final int DEFAULT_INITIAL_SUPPORT = 3000;

    /**
     * The default support threshold
     */
    private static final int DEFAULT_SUPPORT = 3000;

    /**
     * It implements all the operators defined for the mining process: ADD-EDGE,
     * INSTANTIATION, SPECIALIZATION and CLOSE-CIRCLE
     */
    private MiningAssistant assistant;

    /**
     * Support threshold for relations.
     */
    private double minInitialSupport;

    /**
     * Threshold for refinements. It can hold either an absolute support number
     * or a head coverage threshold.
     */
    private double minSignificanceThreshold;

    /**
     * Metric used to prune the mining tree
     */
    private Metric pruningMetric;

    /**
     * Preferred number of threads
     */
    private int nThreads;
    
    /**
     * If true, print the rules as they are discovered.
     */
    private boolean realTime;
    
    /**
     * List of target head relations.
     */
    private Collection<ByteString> seeds;
    
    /**
     * Column headers
     */
    public static final List<String> headers = Arrays.asList("Rule", "Head Coverage", "Std Confidence", 
    		"PCA Confidence", "Positive Examples", "Body size", "PCA Body size",
            "Functional variable", "Std. Lower Bound", "PCA Lower Bound", "PCA Conf estimation");


    /**
     * amount of currently waiting workers
     */
    public static AtomicInteger waitingWorkers = new AtomicInteger(0);

    /**
     *
     * @param assistant An object that implements the logic of the mining operators.
     * @param minInitialSupport If head coverage is defined as pruning metric,
     * it is the minimum size for a relation to be considered in the mining.
     * @param threshold The minimum support threshold: it can be either a head
     * coverage ratio threshold or an absolute number depending on the 'metric' argument.
     * @param metric Head coverage or support.
     */
    public AMIE(MiningAssistant assistant, int minInitialSupport, double threshold, Metric metric, int nThreads) {
        this.assistant = assistant;
        this.minInitialSupport = minInitialSupport;
        this.minSignificanceThreshold = threshold;
        this.pruningMetric = Metric.Support;
        this.nThreads = nThreads;
        this.realTime = true;
        this.seeds = null;
    }

    public MiningAssistant getAssistant() {
        return assistant;
    }
    
	public boolean isVerbose() {
		// TODO Auto-generated method stub
		return assistant.isVerbose();
	}
    
    public boolean isRealTime() {
    	return realTime;
    }
    
    public void setRealTime(boolean realTime) {
    	this.realTime = realTime;
    }

	public Collection<ByteString> getSeeds() {
    	return seeds;
    }
    
    public void setSeeds(Collection<ByteString> seeds) {
    	this.seeds = seeds;
    }

    public double getMinSignificanceThreshold() {
		return minSignificanceThreshold;
	}

	public void setMinSignificanceThreshold(double minSignificanceThreshold) {
		this.minSignificanceThreshold = minSignificanceThreshold;
	}

	public Metric getPruningMetric() {
        return pruningMetric;
    }

    public void setPruningMetric(Metric pruningMetric) {
        this.pruningMetric = pruningMetric;
    }

    public double getMinInitialSupport() {
		return minInitialSupport;
	}

	public void setMinInitialSupport(double minInitialSupport) {
		this.minInitialSupport = minInitialSupport;
	}

	public int getnThreads() {
		return nThreads;
	}

	public void setnThreads(int nThreads) {
		this.nThreads = nThreads;
	}
    

    /**
     * The key method which returns a set of rules mined from the KB based on 
     * the AMIE object's configuration.
     *
     * @return
     * @throws Exception
     */
    public List<Rule> mine() throws Exception {
        List<Rule> result = new ArrayList<>();
        MultiMap<Integer, Rule> indexedResult = new MultiMap<>();
        RuleConsumer consumerObj = null;
        Thread consumerThread = null;
        Lock resultsLock = new ReentrantLock();
        Condition resultsCondVar = resultsLock.newCondition();
        Collection<Rule> seedRules = new ArrayList<>();
        
        // Queue initialization
        if (seeds == null || seeds.isEmpty()) {
            seedRules = assistant.getInitialAtoms(minInitialSupport);
        } else {

            seedRules = assistant.getInitialAtomsFromSeeds(seeds, minInitialSupport);        }

        for(Rule r : seedRules){
            System.out.print("SEED ");
            System.out.println(r.toString());
        }
        AMIEQueue queue = new AMIEQueue(seedRules, nThreads);

        if (realTime) {
            consumerObj = new RuleConsumer(result, resultsLock, resultsCondVar);
            consumerThread = new Thread(consumerObj);
            consumerThread.setName("Consumer");
            consumerThread.start();
        }

        System.out.println("Using " + nThreads + " threads");
        //Create as many threads as available cores
        ArrayList<Thread> currentJobs = new ArrayList<>();
        ArrayList<RDFMinerJob> jobObjects = new ArrayList<>();
        for (int i = 0; i < nThreads; ++i) {
            RDFMinerJob jobObject = new RDFMinerJob(queue, result, resultsLock, resultsCondVar, indexedResult);
            Thread job = new Thread(jobObject);
            job.setName("Miner " + i);
            System.out.println("Miner " + i + " created");
            currentJobs.add(job);

            jobObjects.add(jobObject);

        }

        for (Thread job : currentJobs) {
            job.start();
        }

        for (Thread job : currentJobs) {
            job.join();
        }

        if (realTime) {        
            consumerThread.interrupt();
            while (!consumerThread.isInterrupted());            	
        }

        return result;
    }

    /**
     * It removes and prints rules from a shared list (a list accessed by
     * multiple threads).
     *
     * @author galarrag
     *
     */
    private class RuleConsumer implements Runnable {

        private List<Rule> consumeList;

        private int lastConsumedIndex;

        private Lock consumeLock;

        private Condition conditionVariable;

        public RuleConsumer(List<Rule> consumeList, Lock consumeLock, Condition conditionVariable) {
            this.consumeList = consumeList;
            this.lastConsumedIndex = -1;
            this.consumeLock = consumeLock;
            this.conditionVariable = conditionVariable;
        }

        @Override
        public void run() {
            AMIE.printRuleHeaders(assistant);
            while (!Thread.currentThread().isInterrupted()) {
                consumeLock.lock();
                try {
                    while (lastConsumedIndex == consumeList.size() - 1) {
                        conditionVariable.await();
                        for (int i = lastConsumedIndex + 1; i < consumeList.size(); ++i) {
                        	System.out.println(assistant.formatRule(consumeList.get(i)));
                        }
                        lastConsumedIndex = consumeList.size() - 1;
                    }
                } catch (InterruptedException e) {
                	consumeLock.unlock();
                	System.out.flush();
                	break;
                } 
            }
        }
    }

    /**
     * This class implements the AMIE algorithm in a single thread.
     *
     * @author lgalarra
     */
    private class RDFMinerJob implements Runnable {

        private List<Rule> outputSet;

        // A version of the output set thought for search.
        private MultiMap<Integer, Rule> indexedOutputSet;

        private AMIEQueue queryPool;

        private Lock resultsLock;

        private Condition resultsCondition;

        /**
         * 
         * @param seedsPool
         * @param outputSet
         * @param resultsLock Lock associated to the output buffer were mined rules are added
         * @param resultsCondition Condition variable associated to the results lock
         * @param sharedCounter Reference to a shared counter that keeps track of the number of threads that are running
         * in the system.
         * @param indexedOutputSet
         */
        public RDFMinerJob(AMIEQueue seedsPool,
                List<Rule> outputSet, Lock resultsLock,
                Condition resultsCondition,
                MultiMap<Integer, Rule> indexedOutputSet) {
            this.queryPool = seedsPool;
            this.outputSet = outputSet;
            this.resultsLock = resultsLock;
            this.resultsCondition = resultsCondition;
            this.indexedOutputSet = indexedOutputSet;
        }


        @Override
        public void run() {
            boolean workerWaiting = false;
            boolean shouldrun = true;

            System.out.println("Miner " + Thread.currentThread().getName() + " started and running...");
            while (shouldrun) {
                Rule currentRule = null;
				try {
					currentRule = queryPool.dequeue();

					if(currentRule == null) {
                        // Is worker already waiting?
                        if(workerWaiting == false) {
                            // No - set state to waiting
                            workerWaiting = true;
                            // Increment and get waiting workers
                            int waiting = waitingWorkers.incrementAndGet();
                            //all threads are waiting?
                            if(waiting == nThreads){
                                // All workers can be shutted down
                                shouldrun = false;
                                System.out.println("Miner " + Thread.currentThread().getName() + " all workers are waiting - no more task - shutting down...");
                            }
                        }
                        // Waiting for new possible rules
                        Thread.sleep(50);
					}
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
                 //System.out.println("Current Rule " + currentRule);
               /* if (currentRule == null) {
                	this.queryPool.decrementMaxThreads();
                	break;
                } else
                    */
               if(currentRule != null){
                   System.out.println("QUERY POOL SIZE: " + queryPool.size());
                   //If worker was waiting before
                   if(workerWaiting == true) {
                       //Not waiting anymore
                       workerWaiting = false;
                       //decrement waiting workers
                       waitingWorkers.decrementAndGet();
                   }


                   // Check if the rule meets the language bias and confidence thresholds and
                    // decide whether to output it.
                    boolean outputRule = false;

                    // Here the confidence bounds with our Complete Dataset should be checked!
                   outputRule = assistant.testConfidenceThresholds(currentRule);

                    // Check if we should further refine the rule

                        double threshold = getCountThreshold(currentRule);
                        
                        // Application of the mining operators
                        Map<String, Collection<Rule>> temporalOutputMap = null;
                        try {
                            System.out.println("Apply Mining Operator on " + currentRule.toString());
//                            long start = System.currentTimeMillis();
							temporalOutputMap = assistant.applyMiningOperators(currentRule, threshold);
  //                          long end = System.currentTimeMillis();
  //                          System.out.println("Mining Operators took " + (end-start) + "ms");
						} catch (IllegalAccessException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (IllegalArgumentException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (InvocationTargetException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
                        
                        for (Map.Entry<String, Collection<Rule>> entry : temporalOutputMap.entrySet()) {
                        	String operator = entry.getKey();
                        	Collection<Rule> items = entry.getValue();
                        	//if (!operator.equals("dangling")) {
                            //Add all mined Rules to the queryPool
                            queryPool.queueAll(items);
                        	//}
                        }

                    // Output the rule
                    if (outputRule) {
                        this.resultsLock.lock();
                        Set<Rule> outputQueries = indexedOutputSet.get(currentRule.alternativeParentHashCode());
                        if (outputQueries != null) {
                            if (!outputQueries.contains(currentRule)) {
                                this.outputSet.add(currentRule);
                                outputQueries.add(currentRule);
                            }
                        } else {
                            this.outputSet.add(currentRule);
                            this.indexedOutputSet.put(currentRule.alternativeParentHashCode(), currentRule);
                        }
                        this.resultsCondition.signal();
                        this.resultsLock.unlock();
                    }
                }
            }
        }

        /**
         * Based on AMIE's configuration, it returns the absolute support threshold
         * that should be applied to the rule.
         * @param query
         * @return
         */
        private double getCountThreshold(Rule query) {
            switch (pruningMetric) {
                case Support:
                    return minInitialSupport;
                case HeadCoverage:
                    return Math.ceil((minSignificanceThreshold * 
                    		(double) assistant.getHeadCardinality(query)));
                default:
                    return 0;
            }
        }
    }

    /**
     * Returns an instance of AMIE that mines rules on the given KB using
     * the vanilla setting of head coverage 1% and no confidence threshold.
     * @param db
     * @return
     */
    public static AMIE getVanillaSettingInstance(KB db) {
        return new AMIE(new DefaultMiningAssistant(db),
                100, // Do not look at relations smaller than 100 facts 
                0.01, // Head coverage 1%
                Metric.HeadCoverage,
                Runtime.getRuntime().availableProcessors());
    }
    
    /** Factory methods. They return canned instances of AMIE. **/ 

    /**
     * Returns an instance of AMIE that mines rules on the given KB using
     * the vanilla setting of head coverage 1% and a given PCA confidence threshold
     * @param db
     * @return
     */
    public static AMIE getVanillaSettingInstance(KB db, double minPCAConfidence) {
        DefaultMiningAssistant miningAssistant = new DefaultMiningAssistant(db);
        miningAssistant.setPcaConfidenceThreshold(minPCAConfidence);
        return new AMIE(miningAssistant,
                DEFAULT_INITIAL_SUPPORT, // Do not look at relations smaller than 100 facts 
                DEFAULT_HEAD_COVERAGE, // Head coverage 1%
                Metric.HeadCoverage,
                Runtime.getRuntime().availableProcessors());
    }

    /**
     * Returns an (vanilla setting) instance of AMIE that enables the lossy optimizations, i.e., optimizations that
     * optimize for runtime but that could in principle omit some rules that should be mined.
     * @param db
     * @param minPCAConfidence
     * @param startSupport
     * @return
     */
    public static AMIE getLossyVanillaSettingInstance(KB db, double minPCAConfidence, int startSupport) {
        DefaultMiningAssistant miningAssistant = new DefaultMiningAssistant(db);
        miningAssistant.setPcaConfidenceThreshold(minPCAConfidence);
        miningAssistant.setEnabledConfidenceUpperBounds(true);
        miningAssistant.setEnabledFunctionalityHeuristic(true);
        return new AMIE(miningAssistant,
                startSupport, // Do not look at relations smaller than 100 facts 
                DEFAULT_HEAD_COVERAGE, // Head coverage 1%
                Metric.HeadCoverage,
                Runtime.getRuntime().availableProcessors());
    }

    /**
     * Returns an instance of AMIE that enables the lossy optimizations, i.e., optimizations that
     * optimize for runtime but that could in principle omit some rules that should be mined.
     * @param db
     * @param minPCAConfidence
     * @param minSupport
     * @return
     */
    public static AMIE getLossyInstance(KB db, double minPCAConfidence, int minSupport) {
        DefaultMiningAssistant miningAssistant = new DefaultMiningAssistant(db);
        miningAssistant.setPcaConfidenceThreshold(minPCAConfidence);
        miningAssistant.setEnabledConfidenceUpperBounds(true);
        miningAssistant.setEnabledFunctionalityHeuristic(true);
        return new AMIE(miningAssistant,
                minSupport, // Do not look at relations smaller than the support threshold 
                minSupport, // Head coverage 1%
                Metric.Support,
                Runtime.getRuntime().availableProcessors());
    }
    
    /**
     * Gets an instance of AMIE configured according to the command line arguments.
     * @param args
     * @return
     * @throws IOException
     * @throws InvocationTargetException 
     * @throws IllegalArgumentException 
     * @throws IllegalAccessException 
     * @throws InstantiationException 
     */
    public static ArrayList<AMIE> getInstance(String[] args)
    		throws
            IllegalArgumentException {
        ArrayList<AMIE> minerList = new ArrayList<>();
        List<File> dataFiles = new ArrayList<File>();
        List<File> targetFiles = new ArrayList<File>();
        List<File> schemaFiles = new ArrayList<File>();

        CommandLine cli = null;
        String completePath = "";
        double minStdConf = DEFAULT_STD_CONFIDENCE;
        double minPCAConf = DEFAULT_PCA_CONFIDENCE;
        int minSup = DEFAULT_SUPPORT;
        int minInitialSup = DEFAULT_INITIAL_SUPPORT;
        double minHeadCover = DEFAULT_HEAD_COVERAGE;
        int maxDepth = 3;
        int recursivityLimit = 3;
        boolean realTime = true;
        boolean datalogOutput = true;
        boolean countAlwaysOnSubject = false;
        double minMetricValue = 0.0;
        boolean allowConstants = false;
        boolean enableConfidenceUpperBounds = true;
        boolean enableFunctionalityHeuristic = true;
        boolean verbose = false;
        boolean enforceConstants = false;
        boolean avoidUnboundTypeAtoms = true;
        boolean ommitStdConfidence = false;
        /** System performance measure **/
        boolean exploitMaxLengthForRuntime = true;
        boolean enableQueryRewriting = true;
        boolean enablePerfectRulesPruning = false;
        long sourcesLoadingTime = 0l;
        /*********************************/
        int nProcessors = Runtime.getRuntime().availableProcessors();
        String bias = "default"; // Counting support on the two head variables.
        Metric metric = Metric.HeadCoverage; // Metric used to prune the search space.
        MiningAssistant mineAssistant = null;
        Collection<ByteString> bodyExcludedRelations = null;
        Collection<ByteString> headExcludedRelations = null;
        Collection<ByteString> headTargetRelations = null;
        headTargetRelations = new ArrayList<>();
        headTargetRelations.add(ByteString.of("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"));
        Collection<ByteString> bodyTargetRelations = null;
        KB targetSource = null;
        KB schemaSource = null;
        int nThreads = nProcessors; // By default use as many threads as processors.
        HelpFormatter formatter = new HelpFormatter();

        // create the command line parser
        CommandLineParser parser = new PosixParser();
        // create the Options
        Options options = new Options();

        Option minimalConfidence = OptionBuilder.withArgName("minconf")
                .hasArg()
                .withDescription("Minimum Confidence for Rules that are output.  Default: 0.5")
                .create("minconf");


        Option supportPer = OptionBuilder.withArgName("support")
                .hasArg()
                .withDescription("Minimum Support relative to Number of Entities in provided Class.  Default: 0.1")
                .create("support");

       Option schemaType = OptionBuilder.withArgName("type")
                .hasArg()
                .withDescription("Provide a type/class for the schema that should be mined from the input data.  Default: Person")
                .create("type");



        Option completeKBOpt = OptionBuilder.withArgName("complete")
                .hasArg()
                .withDescription("Complete Knowledgebase for computing schema confidence. Default: null")
                .create("complete");

        Option supportOpt = OptionBuilder.withArgName("min-support")
                .hasArg()
                .withDescription("Minimum absolute support. Default: 100 positive examples")
                .create("mins");

        Option initialSupportOpt = OptionBuilder.withArgName("min-initial-support")
                .hasArg()
                .withDescription("Minimum size of the relations to be considered as head relations. "
                		+ "Default: 100 (facts or entities depending on the bias)")
                .create("minis");

        Option headCoverageOpt = OptionBuilder.withArgName("min-head-coverage")
                .hasArg()
                .withDescription("Minimum head coverage. Default: 0.01")
                .create("minhc");

        Option pruningMetricOpt = OptionBuilder.withArgName("pruning-metric")
                .hasArg()
                .withDescription("Metric used for pruning of intermediate queries: "
                		+ "support|headcoverage. Default: headcoverage")
                .create("pm");

        Option realTimeOpt = OptionBuilder.withArgName("output-at-end")
                .withDescription("Print the rules at the end and not while they are discovered. "
                		+ "Default: false")
                .create("oute");
        
        Option datalogNotationOpt = OptionBuilder.withArgName("datalog-output")
                .withDescription("Print rules using the datalog notation "
                		+ "Default: false")
                .create("datalog");

        Option bodyExcludedOpt = OptionBuilder.withArgName("body-excluded-relations")
                .hasArg()
                .withDescription("Do not use these relations as atoms in the body of rules."
                		+ " Example: <livesIn>,<bornIn>")
                .create("bexr");

        Option headExcludedOpt = OptionBuilder.withArgName("head-excluded-relations")
                .hasArg()
                .withDescription("Do not use these relations as atoms in the head of rules "
                		+ "(incompatible with head-target-relations). Example: <livesIn>,<bornIn>")
                .create("hexr");

        Option headTargetRelationsOpt = OptionBuilder.withArgName("head-target-relations")
                .hasArg()
                .withDescription("Mine only rules with these relations in the head. "
                		+ "Provide a list of relation names separated by commas "
                		+ "(incompatible with head-excluded-relations). "
                		+ "Example: <livesIn>,<bornIn>")
                .create("htr");

        Option bodyTargetRelationsOpt = OptionBuilder.withArgName("body-target-relations")
                .hasArg()
                .withDescription("Allow only these relations in the body. Provide a list of relation "
                		+ "names separated by commas (incompatible with body-excluded-relations). "
                		+ "Example: <livesIn>,<bornIn>")
                .create("btr");

        Option maxDepthOpt = OptionBuilder.withArgName("max-depth")
                .hasArg()
                .withDescription("Maximum number of atoms in the antecedent and succedent of rules. "
                		+ "Default: 3")
                .create("maxad");

        Option pcaConfThresholdOpt = OptionBuilder.withArgName("min-pca-confidence")
                .hasArg()
                .withDescription("Minimum PCA confidence threshold. "
                		+ "This value is not used for pruning, only for filtering of the results. "
                		+ "Default: 0.0")
                .create("minpca");

        Option allowConstantsOpt = OptionBuilder.withArgName("allow-constants")
                .withDescription("Enable rules with constants. Default: false")
                .create("const");

        Option enforceConstantsOpt = OptionBuilder.withArgName("only-constants")
                .withDescription("Enforce constants in all atoms. Default: false")
                .create("fconst");

        Option assistantOp = OptionBuilder.withArgName("e-name")
                .hasArg()
                .withDescription("Syntatic/semantic bias: oneVar|default|[Path to a subclass of amie.mining.assistant.MiningAssistant]"
                		+ "Default: default (defines support and confidence in terms of 2 head variables)")
                .create("bias");

        Option countOnSubjectOpt = OptionBuilder.withArgName("count-always-on-subject")
                .withDescription("If a single variable bias is used (oneVar), "
                		+ "force to count support always on the subject position.")
                .create("caos");

        Option coresOp = OptionBuilder.withArgName("n-threads")
                .hasArg()
                .withDescription("Preferred number of cores. Round down to the actual number of cores "
                		+ "in the system if a higher value is provided.")
                .create("nc");

        Option stdConfThresholdOpt = OptionBuilder.withArgName("min-std-confidence")
                .hasArg()
                .withDescription("Minimum standard confidence threshold. "
                		+ "This value is not used for pruning, only for filtering of the results. Default: 0.0")
                .create("minc");

        Option confidenceBoundsOp = OptionBuilder.withArgName("optim-confidence-bounds")
                .withDescription("Enable the calculation of confidence upper bounds to prune rules.")
                .create("optimcb");

        Option funcHeuristicOp = OptionBuilder.withArgName("optim-func-heuristic")
                .withDescription("Enable functionality heuristic to identify potential low confident rules for pruning.")
                .create("optimfh");

        Option verboseOp = OptionBuilder.withArgName("verbose")
                .withDescription("Maximal verbosity")
                .create("verbose");

        Option recursivityLimitOpt = OptionBuilder.withArgName("recursivity-limit")
                .withDescription("Recursivity limit")
                .hasArg()
                .create("rl");

        Option avoidUnboundTypeAtomsOpt = OptionBuilder.withArgName("avoid-unbound-type-atoms")
                .withDescription("Avoid unbound type atoms, e.g., type(x, y), i.e., bind always 'y' to a type")
                .create("auta");

        Option doNotExploitMaxLengthOp = OptionBuilder.withArgName("do-not-exploit-max-length")
                .withDescription("Do not exploit max length for speedup "
                		+ "(requested by the reviewers of AMIE+). False by default.")
                .create("deml");

        Option disableQueryRewriteOp = OptionBuilder.withArgName("disable-query-rewriting")
                .withDescription("Disable query rewriting and caching.")
                .create("dqrw");

        Option disablePerfectRulesOp = OptionBuilder.withArgName("disable-perfect-rules")
                .withDescription("Disable perfect rules.")
                .create("dpr");

        Option onlyOutputEnhancementOp = OptionBuilder.withArgName("only-output")
                .withDescription("If enabled, it activates only the output enhacements, that is, "
                		+ "the confidence approximation and upper bounds. "
                        + " It overrides any other configuration that is incompatible.")
                .create("oout");

        Option fullOp = OptionBuilder.withArgName("full")
                .withDescription("It enables all enhancements: "
                		+ "lossless heuristics and confidence approximation and upper bounds"
                        + " It overrides any other configuration that is incompatible.")
                .create("full");

        Option extraFileOp = OptionBuilder.withArgName("extraFile")
                .withDescription("An additional text file whose interpretation depends "
                		+ "on the selected mining assistant (bias)")
                .hasArg()
                .create("ef");

        Option calculateStdConfidenceOp = OptionBuilder.withArgName("ommit-std-conf")
        		.withDescription("Do not calculate standard confidence")
        		.create("ostd");

        options.addOption(minimalConfidence);
        options.addOption(supportPer);
        options.addOption(schemaType);
        options.addOption(completeKBOpt);
        options.addOption(stdConfThresholdOpt);
        options.addOption(supportOpt);
        options.addOption(initialSupportOpt);
        options.addOption(headCoverageOpt);
        options.addOption(pruningMetricOpt);
        options.addOption(realTimeOpt);
        options.addOption(bodyExcludedOpt);
        options.addOption(headExcludedOpt);
        options.addOption(maxDepthOpt);
        options.addOption(pcaConfThresholdOpt);
        options.addOption(headTargetRelationsOpt);
        options.addOption(bodyTargetRelationsOpt);
        options.addOption(allowConstantsOpt);
        options.addOption(enforceConstantsOpt);
        options.addOption(countOnSubjectOpt);
        options.addOption(assistantOp);
        options.addOption(coresOp);
        options.addOption(confidenceBoundsOp);
        options.addOption(verboseOp);
        options.addOption(funcHeuristicOp);
        options.addOption(recursivityLimitOpt);
        options.addOption(avoidUnboundTypeAtomsOpt);
        options.addOption(doNotExploitMaxLengthOp);
        options.addOption(disableQueryRewriteOp);
        options.addOption(disablePerfectRulesOp);
        options.addOption(onlyOutputEnhancementOp);
        options.addOption(fullOp);
        options.addOption(extraFileOp);
        options.addOption(datalogNotationOpt);
        options.addOption(calculateStdConfidenceOp);

        try {
            cli = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println("Unexpected exception: " + e.getMessage());
            formatter.printHelp("AMIE [OPTIONS] <TSV FILES>", options);
            System.exit(1);
        }

        // These configurations override any other option
        boolean onlyOutput = cli.hasOption("oout");
        boolean full = cli.hasOption("full");
        if (onlyOutput && full) {
            System.err.println("The options only-output and full are incompatible. Pick either one.");
            formatter.printHelp("AMIE [OPTIONS] <TSV FILES>", options);
            System.exit(1);
        }

        if (cli.hasOption("htr") && cli.hasOption("hexr")) {
            System.err.println("The options head-target-relations and head-excluded-relations cannot appear at the same time");
            System.err.println("AMIE+ [OPTIONS] <.tsv INPUT FILES>");
            System.exit(1);
        }


        if (cli.hasOption("btr") && cli.hasOption("bexr")) {
            System.err.println("The options body-target-relations and body-excluded-relations cannot appear at the same time");
            formatter.printHelp("AMIE+", options);
            System.exit(1);
        }

        if (cli.hasOption("mins")) {
            String minSupportStr = cli.getOptionValue("mins");
            try {
                minSup = Integer.parseInt(minSupportStr);
            } catch (NumberFormatException e) {
                System.err.println("The option -mins (support threshold) requires an integer as argument");
                System.err.println("AMIE+ [OPTIONS] <.tsv INPUT FILES>");
                formatter.printHelp("AMIE+", options);
                System.exit(1);
            }
        }

        if (cli.hasOption("complete")) {
            System.out.println("Complete is activated");
                completePath = cli.getOptionValue("complete");
                CLUSTER_MODE = true;
        }

        if (cli.hasOption("type")) {
            type = cli.getOptionValue("type");
            System.out.println("Mine Schema Patterns for " + type);
        }

        if (cli.hasOption("support")) {
            try{
                supportPercentage = Double.valueOf(cli.getOptionValue("support"));
            }catch (NumberFormatException e){
                System.err.println("The option -support (minimum relative support) requires a double as argument");
                System.exit(1);
            }
            System.out.println("Relative Support is " + supportPercentage);
        }

        if (cli.hasOption("minconf")) {
            try{
                minConfidence = Double.valueOf(cli.getOptionValue("support"));
            }catch (NumberFormatException e){
                System.err.println("The option -minconf (minimum confidence) requires a double as argument");
                System.exit(1);
            }
            System.out.println("Minimum Rule Confidence is " + minConfidence);
        }

        if (cli.hasOption("minis")) {
            String minInitialSupportStr = cli.getOptionValue("minis");
            try {
                minInitialSup = Integer.parseInt(minInitialSupportStr);
            } catch (NumberFormatException e) {
                System.err.println("The option -minis (initial support threshold) requires an integer as argument");
                System.err.println("AMIE+ [OPTIONS] <.tsv INPUT FILES>");
                formatter.printHelp("AMIE+", options);
                System.exit(1);
            }
        }






        if (cli.hasOption("nc")) {
            String nCoresStr = cli.getOptionValue("nc");
            try {
                nThreads = Integer.parseInt(nCoresStr);
            } catch (NumberFormatException e) {
                System.err.println("The argument for option -nc (number of threads) must be an integer");
                System.err.println("AMIE [OPTIONS] <.tsv INPUT FILES>");
                System.err.println("AMIE+ [OPTIONS] <.tsv INPUT FILES>");
                System.exit(1);
            }

            if (nThreads > nProcessors) {
                nThreads = nProcessors;
            }
        }

        String[] leftOverArgs = cli.getArgs();

        if (leftOverArgs.length < 1) {
            System.err.println("No input file has been provided");
            System.err.println("AMIE [OPTIONS] <.tsv INPUT FILES>");
            System.err.println("AMIE+ [OPTIONS] <.tsv INPUT FILES>");
            System.exit(1);
        }

        //Load database
        for (int i = 0; i < leftOverArgs.length; ++i) {
            if (leftOverArgs[i].startsWith(":t")) {
                targetFiles.add(new File(leftOverArgs[i].substring(2)));
            } else if (leftOverArgs[i].startsWith(":s")) {
                schemaFiles.add(new File(leftOverArgs[i].substring(2)));
            } else {
                dataFiles.add(new File(leftOverArgs[i]));
            }
        }


        //We build an array list of input files for each cluster group and add these to an arraylist of KBs
        ArrayList<KB> dataSources = new ArrayList<>();
        for(File f : dataFiles){
            KB dataSource = new KB(minInitialSup);
            dataSource.load(f);
            dataSource.summarize(false);
            dataSources.add(dataSource);
        }

        allowConstants = cli.hasOption("const");
        countAlwaysOnSubject = cli.hasOption("caos");
        realTime = !cli.hasOption("oute");
        enforceConstants = cli.hasOption("fconst");
        datalogOutput = cli.hasOption("datalog");
        ommitStdConfidence = cli.hasOption("ostd");

        // These configurations override others
        if (onlyOutput) {
            System.out.println("Using the only output enhacements configuration.");
            enablePerfectRulesPruning = false;
            enableQueryRewriting = false;
            exploitMaxLengthForRuntime = false;
            enableConfidenceUpperBounds = true;
            enableFunctionalityHeuristic = true;
            minPCAConf = DEFAULT_PCA_CONFIDENCE;
        }

        if (full) {
            System.out.println("Using the FULL configuration.");
            enablePerfectRulesPruning = true;
            enableQueryRewriting = true;
            exploitMaxLengthForRuntime = true;
            enableConfidenceUpperBounds = true;
            enableFunctionalityHeuristic = true;
            minPCAConf = DEFAULT_PCA_CONFIDENCE;
        }

        for (KB kb : dataSources) {

                int rareRelationshipSup = (int) (kb.entitiesSize() * 0.01);
                kb.setMinimumRelationshipSupport(rareRelationshipSup);


                kb.summarize(false);
                System.out.println(kb.object2relation2subject.get(ByteString.of(type)).size());
                int sup = (int)(kb.object2relation2subject.get(ByteString.of(type)).get(ByteString.of("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")).size() * supportPercentage);
                kb.removeFrequentRelationships((int)(kb.object2relation2subject.get(ByteString.of(type)).get(ByteString.of("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")).size()*0.9));
                System.out.println("minimum Support is " + sup);
                minInitialSup = sup;
                mineAssistant = new SchemaAttributeMiningAssistant(kb, type);

            mineAssistant.setKbSchema(schemaSource);
            mineAssistant.setEnabledConfidenceUpperBounds(enableConfidenceUpperBounds);
            mineAssistant.setEnabledFunctionalityHeuristic(enableFunctionalityHeuristic);
            mineAssistant.setMaxDepth(maxDepth);
            mineAssistant.setStdConfidenceThreshold(minStdConf);
            mineAssistant.setPcaConfidenceThreshold(minPCAConf);
            mineAssistant.setAllowConstants(allowConstants);
            mineAssistant.setEnforceConstants(enforceConstants);
            mineAssistant.setBodyExcludedRelations(bodyExcludedRelations);
            mineAssistant.setHeadExcludedRelations(headExcludedRelations);
            mineAssistant.setTargetBodyRelations(bodyTargetRelations);
            mineAssistant.setCountAlwaysOnSubject(countAlwaysOnSubject);
            mineAssistant.setRecursivityLimit(recursivityLimit);
            mineAssistant.setAvoidUnboundTypeAtoms(avoidUnboundTypeAtoms);
            mineAssistant.setExploitMaxLengthOption(exploitMaxLengthForRuntime);
            mineAssistant.setEnableQueryRewriting(enableQueryRewriting);
            mineAssistant.setEnablePerfectRules(enablePerfectRulesPruning);
            mineAssistant.setVerbose(verbose);
            mineAssistant.setOmmitStdConfidence(ommitStdConfidence);
            mineAssistant.setDatalogNotation(datalogOutput);

            System.out.println(mineAssistant.getDescription());

            AMIE miner = new AMIE(mineAssistant, minInitialSup, minMetricValue, metric, nThreads);
            miner.setRealTime(realTime);
            miner.setSeeds(headTargetRelations);

            minerList.add(miner);
        }


        return minerList;
    }
    
	private static void printRuleHeaders(MiningAssistant assistant) {
		List<String> finalHeaders = new ArrayList<>(headers);
		if (assistant.isOmmitStdConfidence()) {
			finalHeaders.removeAll(Arrays.asList("Std Confidence", "Body size"));
		}
				
		if (!assistant.isVerbose()) {
			finalHeaders.removeAll(Arrays.asList("Std. Lower Bound", "PCA Lower Bound", "PCA Conf estimation"));
        }
		
    	System.out.println(telecom.util.collections.Collections.implode(finalHeaders, "\t"));
	}


	/**
	 * AMIE's main program
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

    	amie.data.U.loadSchemaConf();
    	System.out.println("Assuming " + amie.data.U.typeRelationBS + " as type relation");
    	long loadingStartTime = System.currentTimeMillis();
        ArrayList<AMIE> miners = AMIE.getInstance(args);
        AMIE miner = miners.get(0);
        MiningAssistant assistant = miner.getAssistant();
        miner.mine();
    }

}
