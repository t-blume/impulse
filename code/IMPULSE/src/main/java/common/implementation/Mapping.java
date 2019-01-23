package main.java.common.implementation;


import org.json.JSONArray;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.Set;


public class Mapping {


    public Mapping(String jsonString){
        JSONObject jsonObject = new JSONObject(jsonString);

        JSONArray queriesJSON = jsonObject.getJSONArray("queries");
        queries = new HashSet<>();
        queriesJSON.forEach(T -> queries.add(T.toString()));

        JSONArray typesJSON = jsonObject.getJSONArray("types");
        types = new HashSet<>();
        typesJSON.forEach(T -> types.add(T.toString()));

        JSONArray mandatoryMappingsJSON = jsonObject.getJSONArray("properties");
        properties = new HashSet<>();
        mandatoryMappingsJSON.forEach(T -> properties.add(T.toString()));

        this.mappings = jsonObject.getJSONObject("mappings");
    }


    private Set<String> queries = null;

    private Set<String> types = null;

    private Set<String> properties = null;

    private JSONObject mappings = null;



    public Set<String> getQueries() {
        return queries;
    }

    public void setQueries(Set<String> queries) {
        this.queries = queries;
    }

    public Set<String> getTypes() {
        return types;
    }

    public void setTypes(Set<String> types) {
        this.types = types;
    }

    public Set<String> getProperties() {
        return properties;
    }

    public void setProperties(Set<String> properties) {
        this.properties = properties;
    }

    public JSONObject getMappings() {
        return mappings;
    }

    public void setMappings(JSONObject mappings) {
        this.mappings = mappings;
    }




}
