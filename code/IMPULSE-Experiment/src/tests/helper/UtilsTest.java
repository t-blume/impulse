package helper;

import junit.framework.TestCase;


import java.net.URISyntaxException;

public class UtilsTest extends TestCase {

    public void testSortByValue() {
    }

    public void testNormalizeURL() throws URISyntaxException {
        String[][] testUrls = new String[][]{
                {
                        "glottolog.org", "glottolog.org"
                },
                {
                        "https://www.jetbrains.com/help/idea/create-tests.html", "jetbrains.com/help/idea/create-tests.html"
                },
                {
                        "http://bnb.data.bl.uk/doc/resource/009538538/", "bnb.data.bl.uk/doc/resource/009538538"
                },
                {
                        "http://bnb.data.bl.uk/doc/resource/009538538#", "bnb.data.bl.uk/doc/resource/009538538"
                }
        };

        for (String[] testUrl : testUrls) {
            assertEquals(testUrl[1], Utils.normalizeURL(testUrl[0]));
        }
    }

    public void testCompareTitles() {
        String[][] sameTitles = new String[][]{
                {
                        "Garages & vehicle service centres", "Garages & vehicle service centres"
                },
                {
                        "The Children (Scotland) Act 1995 : regulations and guidance", "The Children (Scotland) Act 1995 : regulations and guidance"
                }
        };

        String[][] differentTitles = new String[][]{
                {
                        "Ghana", "Ghana : country assistance plan"
                },
                {
                        "Garages & vehicle service centres", "Garages & plains service centres"
                },
                {
                        "The Children (Scotland) Act 1999 : regulations and guidance", "The Children (Scotland) Act 1995 : regulations and guidance"
                },
                {
                        "Trinity College, Carmarthen : humanities, January 1995 : quality assessment report", "Interventions for children and young people with drug-misusing carers : final report to the Department of Health"
                },
                {
                        "Secure accommodation at Silverbrook : report of an inspection November 1995", "Housing energy and conservation report '98"
                }
        };

//        for(String[] title : sameTitles){
//            assertTrue(Utils.compareTitles(title[0], title[1]));
//            assertTrue(Utils.compareTitles(title[1], title[0]));
//        }

        for (String[] title : differentTitles) {
            assertFalse(Utils.compareTitles(title[0], title[1]));
            assertFalse(Utils.compareTitles(title[1], title[0]));
        }
    }

    public void testCompareAuthors() {
    }

    public void testNormalizeStrings() {
    }


    public void testExtractPLD() throws URISyntaxException {
        String[][] testUrls = new String[][]{
                {
                        "glottolog.org", "glottolog.org"
                },
                {
                        "https://www.jetbrains.com/help/idea/create-tests.html", "jetbrains.com"
                },
                {
                        "http://bnb.data.bl.uk/doc/resource/009538538/", "bnb.data.bl.uk"
                },
                {
                        "bnb.data.bl.uk/doc/resource/009538538#", "bnb.data.bl.uk"
                }
        };

        for(String[] testUrl : testUrls){
            assertEquals(testUrl[1], Utils.extractPLD(Utils.normalizeURL(testUrl[0])));
        }
    }
}