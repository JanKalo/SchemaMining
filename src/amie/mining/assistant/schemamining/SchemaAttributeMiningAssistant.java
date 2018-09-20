package amie.mining.assistant.schemamining;

import amie.data.KB;
import amie.mining.AMIE;
import amie.mining.assistant.MiningAssistant;
import amie.mining.assistant.MiningOperator;
import amie.rules.Rule;
import javatools.database.Virtuoso;
import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;

import java.util.*;

public class SchemaAttributeMiningAssistant extends MiningAssistant {

    ByteString concept;
    //static KB complete;
    static Virtuoso virtuoso = new Virtuoso();
    static Object myLock = new Object();
    int classSize;






    public SchemaAttributeMiningAssistant(KB dataSource, KB completeKB) {
        super(dataSource);
        virtuoso = new Virtuoso();
        //bodyExcludedRelations = Arrays.asList(ByteString.of("<rdf:type>"));
        super.maxDepth = 8;
        super.setEnablePerfectRules(true);
        super.minPcaConfidence = 0.05;
        super.minStdConfidence = 0.001;
        //Class in Head Relationship that should be mined
        this.concept = ByteString.of("http://dbpedia.org/ontology/City");
        super.bodyExcludedRelations = Arrays.asList(ByteString.of(super.typeRelationship));
        //this.complete = completeKB;

    }


    public SchemaAttributeMiningAssistant(KB dataSource, String type, String typeRelation) {
        super(dataSource);
        //virtuoso = new Virtuoso();
        //bodyExcludedRelations = Arrays.asList(ByteString.of("<rdf:type>"));
        super.typeRelationship = typeRelation;
        super.maxDepth = 8;
        super.setEnablePerfectRules(true);
        super.minPcaConfidence = 0.05;
        super.minStdConfidence = 0.001;
        //Class in Head Relationship that should be mined
        this.concept = ByteString.of(type);
        super.bodyExcludedRelations = Arrays.asList(ByteString.of(super.typeRelationship), ByteString.of("http://purl.org/dc/terms/subject"));
        this.classSize = dataSource.object2relation2subject.get(ByteString.of(type)).get(ByteString.of(super.typeRelationship)).size();


    }



    @Override
    public String getDescription() {
        return "Rules of the form r(x,y) r(x,z) => type(x, C) or r(x,c1) r(x,c2) => type(x, C)";
    }



    @Override
    public Collection<Rule> getInitialAtomsFromSeeds(Collection<ByteString> relations,
                                                     double minSupportThreshold) {
        ByteString relation = relations.iterator().next();
        Collection<Rule> output = new ArrayList<>();
        Rule emptyQuery = new Rule();
        ByteString[] newEdge = emptyQuery.fullyUnboundTriplePattern();
        emptyQuery.getTriples().add(newEdge);
        newEdge[1] = relation;
        int countVarPos = countAlwaysOnSubject ? 0 : findCountingVariable(newEdge);
        ByteString countingVariable = newEdge[countVarPos];
        long cardinality = kb.countDistinct(countingVariable, emptyQuery.getTriples());
        ByteString[] succedent = newEdge.clone();
        Rule candidate = new Rule(succedent, cardinality);
        candidate.setFunctionalVariablePosition(countVarPos);
        registerHeadRelation(candidate);
        ByteString[] danglingEdge = candidate.getTriples().get(0);
        IntHashMap<ByteString> constants = kb.frequentBindingsOf(danglingEdge[2], candidate.getFunctionalVariable(), candidate.getTriples());
        cardinality = constants.get(this.concept);

        Rule newCandidate = candidate.instantiateConstant(2, this.concept, cardinality);
        output.add(newCandidate);
        System.out.println("SELECT DISTINCT");
        System.out.println(newCandidate);

        return output;
    }


    @MiningOperator(name = "dangling")
    public void getDanglingAtoms(Rule rule, double minSupportThreshold, Collection<Rule> output) {
        ByteString[] newEdge = rule.fullyUnboundTriplePattern();

        List<ByteString> joinVariables = null;


        //Bind all Atoms to central entity variable
        joinVariables = rule.getVariables();
        ByteString joinVariable = joinVariables.get(0);

        int nPatterns = rule.getTriples().size();
        ByteString originalRelationVariable = newEdge[1];

        for (int joinPosition = 0; joinPosition <= 2; joinPosition += 2) {
            ByteString originalFreshVariable = newEdge[joinPosition];


            newEdge[joinPosition] = joinVariable;
            rule.getTriples().add(newEdge);
            IntHashMap<ByteString> promisingRelations = kb.frequentBindingsOf(newEdge[1],
                    rule.getFunctionalVariable(), rule.getTriples());
            rule.getTriples().remove(nPatterns);

            int danglingPosition = (joinPosition == 0 ? 2 : 0);

            for (ByteString relation : promisingRelations) {
                if (this.bodyExcludedRelations != null &&
                        this.bodyExcludedRelations.contains(relation))
                    continue;
                //Here we still have to make a redundancy check
                int cardinality = promisingRelations.get(relation);


                //check weather the cardinality of the joined relationship is above the minimum Threshold
                //Problems: Isnt the Support between 0 and 1, and the cardinality always above 1?
                System.out.println("Rule" + rule + "Relation " + relation + "Cardinality " + cardinality);
                if (cardinality >= minSupportThreshold) {

                    if (!rule.containsRelation(relation)) {
                        newEdge[1] = relation;
                        Rule candidate = rule.addAtom(newEdge, cardinality);
                        if (candidate.containsUnifiablePatterns()) {
                            //Verify whether dangling variable unifies to a single value (I do not like this hack)
                            if (kb.countDistinct(newEdge[danglingPosition], candidate.getTriples()) < 2)
                                continue;
                        }

                        candidate.setHeadCoverage(candidate.getSupport()
                                / headCardinalities.get(candidate.getHeadRelation()));
                        candidate.setSupportRatio(candidate.getSupport()
                                / (double) getTotalCount(candidate));
                        candidate.addParent(rule);

                        candidate.setFrequency(cardinality/(double) this.classSize);

                        //Here we add the generated candidate to the output collection
                        output.add(candidate);

                    }
                }

                //}

                newEdge[1] = originalRelationVariable;
            }
            newEdge[joinPosition] = originalFreshVariable;

        }
    }

    @Override
    public boolean testConfidenceThresholds(Rule candidate) {
            candidate.setClassConfidence(this.getClassConfidence(candidate));
            System.out.println(candidate);
            System.out.println(candidate.getClassConfidence());
            return candidate.getClassConfidence() > AMIE.minConfidence;
    }

    @Override
    public void getClosingAtoms(Rule query, double minSupportThreshold, Collection<Rule> output) {
        return;
    }

    @Override
    public void getInstantiatedAtoms(Rule rule, double minSupportThreshold,
                                     Collection<Rule> danglingEdges, Collection<Rule> output) {
        return;
    }

    @Override
    protected void getInstantiatedAtoms(Rule queryWithDanglingEdge, Rule parentQuery,
                                        int danglingAtomPosition, int danglingPositionInEdge, double minSupportThreshold, Collection<Rule> output) {

        ByteString[] danglingEdge = queryWithDanglingEdge.getTriples().get(danglingAtomPosition);
        IntHashMap<ByteString> constants = kb.frequentBindingsOf(danglingEdge[danglingPositionInEdge],
                queryWithDanglingEdge.getFunctionalVariable(), queryWithDanglingEdge.getTriples());
        for (ByteString constant: constants){
            int cardinality = constants.get(constant);
            if(cardinality >= minSupportThreshold){
                ByteString[] lastPatternCopy = queryWithDanglingEdge.getLastTriplePattern().clone();
                lastPatternCopy[danglingPositionInEdge] = constant;
                Rule candidate = queryWithDanglingEdge.instantiateConstant(danglingPositionInEdge,
                        constant, cardinality);

                if(candidate.getRedundantAtoms().isEmpty()){
                    candidate.setHeadCoverage((double)cardinality / headCardinalities.get(candidate.getHeadRelation()));
                    candidate.setSupportRatio((double)cardinality / (double)getTotalCount(candidate));

                    candidate.setFrequency(cardinality/(double) this.classSize);
                    candidate.addParent(parentQuery);
                    output.add(candidate);
                }
            }
        }
    }



    public double getFrequency(Rule r){
        double frequency = 0.0;
        ByteString[] head = r.getHead();
        ByteString countVariable = null;
        countVariable = head[0];
        long support = kb.countDistinct(countVariable, r.getTriples());

        frequency = support/(double) this.classSize;
        return frequency;
    }



    /**
     * A method that computes the specificity of a rule regarding the input class.
     *
     * @param  r
     * @return Returns Schema Rule Confidence classConfidence
     */

    public double getClassConfidence(Rule r){
        double classConfidence = 0.0;
        ByteString[] head = r.getHead();


        long supportComplete;
        long support;
        synchronized (myLock) {
            support = virtuoso.getResultSize(r.toSPARQL());
            supportComplete = virtuoso.getResultSize(r.bodyToSparql());
        }
        classConfidence = support/(double) supportComplete;
        return classConfidence;
    }
}
