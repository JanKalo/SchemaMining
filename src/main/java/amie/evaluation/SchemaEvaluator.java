package amie.evaluation;

import amie.rules.Rule;
import javatools.database.Virtuoso;

import java.util.HashSet;
import java.util.Set;

public class SchemaEvaluator {

    static Virtuoso virtuoso;
    private Set<Rule> rules;
    Set<String> entities;


    public SchemaEvaluator(Set<Rule> rules){
        virtuoso = new Virtuoso();
        this.rules = rules;
        entities = new HashSet<>();

    }

    public void evaluateRule(Rule rule){
        entities.addAll(virtuoso.getResults(rule.bodyToSparql()));


    }

}
