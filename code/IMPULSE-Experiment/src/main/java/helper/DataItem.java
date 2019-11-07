package helper;

import java.net.URISyntaxException;
import java.util.*;

public class DataItem {
    public enum InputType {
        MOVING, ZBW
    }

    public void parseZBW(Map<String, Object> sourceMap, String id) {
        _id = id;
        _title = (String) sourceMap.get("title");
        ArrayList<String> tmpAbstract = (ArrayList<String>) sourceMap.get("abstract");
        if (tmpAbstract != null) {
            if (tmpAbstract.size() > 1)
                System.out.println("Long abstract: " + id);

            _abstract = tmpAbstract.get(0);
        }

        _authorList = new HashSet<>();

        List<HashMap> authorList = (List) sourceMap.get("metadata_persons");
        if (authorList != null)
            authorList.forEach(o -> _authorList.add(new

                    Person((String) o.

                    get("rawName"))));


        _concepts = new HashSet<>();
        ArrayList<String> conceptList = (ArrayList<String>) sourceMap.get("subject_byAuthor");
//        List<String> conceptList = new ArrayList<String>(Arrays.asList(concepts.substring(1, concepts.length() - 1).split(", ")));
        if (conceptList != null)
            conceptList.forEach(o ->

            {
                _concepts.add(new Concept("", Utils.normalizeStrings(o)));
            });
        _sourceURLs = new HashSet<>();
        _sourceURLs.add("zbw.eu");
//        if (sourceMap.get("source_name") != null) {
//            System.out.println(sourceMap.get("source_name"));
////            List<String> sourceURIs = (List) sourceMap.get("sourceURLs");
////            for (String sourceURI : sourceURIs) {
////                try {
////                    _sourceURLs.add(Utils.normalizeURL(sourceURI));
////                } catch (URISyntaxException e) {
////                    e.printStackTrace();
////                    System.exit(0);
////                }
////            }
//        }
    }

    public void parseMOVING(Map<String, Object> sourceMap, String id) {
        if (sourceMap == null)
            return;

        _id = id;
        _title = (String) sourceMap.get("title");
        _abstract = (String) sourceMap.get("abstract");

        _authorList = new HashSet<>();

        List<HashMap> authorList = (List) sourceMap.get("creator_personal");
        if (authorList != null)
            authorList.forEach(o -> _authorList.add(new Person((String) o.get("name"))));


        _concepts = new HashSet<>();
        List<HashMap> conceptList = (List) sourceMap.get("concepts");
        if (conceptList != null)
            conceptList.forEach(o -> {
                try {
                    _concepts.add(new Concept(Utils.normalizeURL((String) o.get("URL")), Utils.normalizeStrings((String) o.get("label"))));
                } catch (URISyntaxException e) {
                    e.printStackTrace();
                }
            });

        if (sourceMap.get("sourceURLs") != null) {
            List<String> sourceURIs = (List) sourceMap.get("sourceURLs");
            for (String sourceURI : sourceURIs) {
                try {
                    _sourceURLs.add(Utils.normalizeURL(sourceURI));
                } catch (URISyntaxException e) {
                    e.printStackTrace();
                    System.exit(0);
                }
            }
        }

    }

    public Map<String, Object> reverseParseMOVING() {

        Map<String, Object> document = new HashMap<>();

        document.put("title", _title);
        if (_abstract != null)
            document.put("abstract", _abstract);

        List<HashMap> authorList = new LinkedList<>();

        _authorList.forEach(author -> {
            HashMap<String, Object> authorMap = new HashMap<>();
            authorMap.put("rawName", author._rawName);
            authorList.add(authorMap);
        });

        document.put("metadata_persons", authorList);

        document.put("sourceURLs", _sourceURLs);


        List<HashMap> conceptList = new LinkedList<>();

        _concepts.forEach(concept -> {
            HashMap<String, Object> conceptMap = new HashMap<>();
            conceptMap.put("URL", concept._URL);
            conceptMap.put("label", concept._label);
            conceptList.add(conceptMap);
        });
        document.put("concepts", conceptList);

        return document;
    }


    public Map<String, Object> reverseParseZBW() {

        Map<String, Object> document = new HashMap<>();

        document.put("title", _title);
        if (_abstract != null)
            document.put("abstract", _abstract);

        List<HashMap> authorList = new LinkedList<>();

        _authorList.forEach(author -> {
            HashMap<String, Object> authorMap = new HashMap<>();
            authorMap.put("name", author._rawName);
            authorList.add(authorMap);
        });

        document.put("creator_personal", authorList);

        document.put("sourceURLs", _sourceURLs);


        List<HashMap> conceptList = new LinkedList<>();

        _concepts.forEach(concept -> {
            HashMap<String, Object> conceptMap = new HashMap<>();
            conceptMap.put("URL", concept._URL);
            conceptMap.put("label", concept._label);
            conceptList.add(conceptMap);
        });
        document.put("concepts", conceptList);

        return document;
    }


    //////////////////////////
    public String _id = "";
    public String _title = "";
    public Set<Person> _authorList = new HashSet<>();
    public String _abstract;
    public Set<String> _keywords;
    public Set<Concept> _concepts;
    public Set<String> _sourceURLs = new HashSet<>();


    public void merge(DataItem otherItem) {
        if (otherItem == null)
            return;
        if (_abstract == null)
            _abstract = otherItem._abstract;
        else if (otherItem._abstract != null)
            if (_abstract.length() < otherItem._abstract.length())
                _abstract = otherItem._abstract;

        if (_keywords == null)
            _keywords = otherItem._keywords;
        else if (otherItem._keywords != null)
            for (String otherKeyword : otherItem._keywords)
                if (otherKeyword != null)
                    _keywords.add(otherKeyword);


        if (_concepts == null)
            _concepts = otherItem._concepts;
        else if (otherItem._concepts != null)
            for (Concept otherConcept : otherItem._concepts)
                if (otherConcept != null)
                    _concepts.add(otherConcept);

        if (_sourceURLs == null)
            _sourceURLs = otherItem._sourceURLs;
        else if (otherItem._sourceURLs != null)
            for (String otherSource : otherItem._sourceURLs)
                if (otherSource != null)
                    _sourceURLs.add(otherSource);

    }


    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("(" + _id + ") ");
        for (Person author : _authorList) {
            if (author != null)
                stringBuilder.append(author._rawName + ",");

        }
        stringBuilder.append(": " + _title);
        return stringBuilder.toString();
    }


    public class Concept {
        public String _URL;
        public String _label;

        public Concept(String url, String label) {
            this._URL = url;
            this._label = label;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Concept concept = (Concept) o;
            return Objects.equals(_URL, concept._URL) &&
                    Objects.equals(_label, concept._label);
        }

        @Override
        public int hashCode() {
            return Objects.hash(_URL, _label);
        }
    }

    public class Person {
        public String _URL;
        public String _name;
        public String _rawName;

        public Person(String rawName) {
            this._rawName = rawName;
        }

    }
}
