package com.bigdata.es.laosiji.estools.esdoc;

import com.vividsolutions.jts.geom.Coordinate;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.MultiPointBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilders;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import javax.swing.plaf.multi.MultiMenuItemUI;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.*;

public class EsDsl {
    /**
     * 查询所有
     *
     * @return
     */
    public static QueryBuilder MulitSearch() {
        QueryBuilder qb = matchAllQuery();
        return qb;
    }


    /**
     * full text query
     * <p>
     * Full text queriesedit
     * The high-level full text queries are usually used for running full text queries on full text fields like the body of an email. They understand how the field being queried is analyzed and will apply each field’s analyzer (or search_analyzer) to the query string before executing.
     * <p>
     * The queries in this group are:
     * <p>
     * match query
     * The standard query for performing full text queries, including fuzzy matching and phrase or proximity queries.
     * multi_match query
     * The multi-field version of the match query.
     * common_terms query
     * A more specialized query which gives more preference to uncommon words.
     * query_string query
     * Supports the compact Lucene query string syntax, allowing you to specify AND|OR|NOT conditions and multi-field search within a single query string. For expert users only.
     * simple_query_string
     * A simpler, more robust version of the query_string syntax suitable for exposing directly to users.
     *
     * @param filename
     * @param keywords
     * @return
     */
    public static QueryBuilder MatchSearch(String filename, String keywords) {
        QueryBuilder qb = matchQuery(filename, keywords);
        return qb;
    }

    public static QueryBuilder MulitMatchQuery(String filename1, String filename2, String keywords1, String keywords2) {
        QueryBuilder qb = multiMatchQuery(keywords1 += keywords2, filename1, filename2);
        return qb;
    }

    public static QueryBuilder CommonTermsQuery(String filename, String keywords) {
        QueryBuilder qb = commonTermsQuery(filename, keywords);
        return qb;
    }

    public static QueryBuilder QueryStringQuery(String keywords) {
        QueryBuilder qb = queryStringQuery(keywords);
        return qb;
    }

    public static QueryBuilder simpleQueryStringQuery(String keywords) {
        QueryBuilder qb = queryStringQuery(keywords);
        return qb;
    }

    /**
     * terquery
     * <p>
     * While the full text queries will analyze the query string before executing, the term-level queries operate on the exact terms that are stored in the inverted index.
     * <p>
     * These queries are usually used for structured data like numbers, dates, and enums, rather than full text fields. Alternatively, they allow you to craft low-level queries, foregoing the analysis process.
     * <p>
     * The queries in this group are:
     * <p>
     * term query
     * Find documents which contain the exact term specified in the field specified.
     * terms query
     * Find documents which contain any of the exact terms specified in the field specified.
     * range query
     * Find documents where the field specified contains values (dates, numbers, or strings) in the range specified.
     * exists query
     * Find documents where the field specified contains any non-null value.
     * prefix query
     * Find documents where the field specified contains terms which being with the exact prefix specified.
     * wildcard query
     * Find documents where the field specified contains terms which match the pattern specified, where the pattern supports single character wildcards (?) and multi-character wildcards (*)
     * regexp query
     * Find documents where the field specified contains terms which match the regular expression specified.
     * fuzzy query
     * Find documents where the field specified contains terms which are fuzzily similar to the specified term. Fuzziness is measured as a Levenshtein edit distance of 1 or 2.
     * type query
     * Find documents of the specified type.
     * ids query
     * Find documents with the specified type and IDs.
     *
     * @param filename
     * @param keywords
     * @return
     */

    public static QueryBuilder termQuery(String filename, String keywords) {
        QueryBuilder qb = termQuery(filename, keywords);
        return qb;
    }


    /**
     * Compound queriesedit
     * Compound queries wrap other compound or leaf queries, either to combine their results and scores, to change their behaviour, or to switch from query to filter context.
     * <p>
     * The queries in this group are:
     * <p>
     * constant_score query
     * A query which wraps another query, but executes it in filter context. All matching documents are given the same “constant” _score.
     * bool query
     * The default query for combining multiple leaf or compound query clauses, as must, should, must_not, or filter clauses. The must and should clauses have their scores combined — the more matching clauses, the better — while the must_not and filter clauses are executed in filter context.
     * dis_max query
     * A query which accepts multiple queries, and returns any documents which match any of the query clauses. While the bool query combines the scores from all matching queries, the dis_max query uses the score of the single best- matching query clause.
     * function_score query
     * Modify the scores returned by the main query with functions to take into account factors like popularity, recency, distance, or custom algorithms implemented with scripting.
     * boosting query
     * Return documents which match a positive query, but reduce the score of documents which also match a negative query.
     * indices query
     * Execute one query for the specified indices, and another for other indices.
     * @param
     */

    public static ConstantScoreQueryBuilder constantScoreQuery(String filename, String keywords) {
      /*  QueryBuilder qb = boolQuery()
                .must(termQuery("content", "test1"))
                .must(termQuery("content", "test4"))
                .mustNot(termQuery("content", "test2"))
                .should(termQuery("content", "test3"))
                .filter(termQuery("content", "test5"));*/
        return new ConstantScoreQueryBuilder(termQuery(filename,keywords).boost(2.0f));
  }


    /**
     * join query
     * Performing full SQL-style joins in a distributed system like Elasticsearch is prohibitively expensive. Instead, Elasticsearch offers two forms of join which are designed to scale horizontally.
     nested query
     Documents may contains fields of type nested. These fields are used to index arrays of objects, where each object can be queried (with the nested query) as an independent document.
     has_child and has_parent queries
     A parent-child relationship can exist between two document types within a single index. The has_child query returns parent documents whose child documents match the specified query, while the has_parent query returns child documents whose parent document matches the specified query.
     * @param obj1
     * @param blue
     * @param avg
     * @return
     */
  public static QueryBuilder nestedQuery(String obj1, BoolQueryBuilder blue, ScoreMode avg){
      QueryBuilder qb = nestedQuery("obj1", boolQuery().must(matchQuery("obj1.name", "blue")).must(rangeQuery("obj1.count").gt(5)), ScoreMode.Avg);
      return qb;
  }

    /**
     * Geo queriesedit
     Elasticsearch supports two types of geo data: geo_point fields which support lat/lon pairs, and geo_shape fields, which support points, lines, circles, polygons, multi-polygons etc.

     The queries in this group are:

     geo_shape query
     Find document with geo-shapes which either intersect, are contained by, or do not intersect with the specified geo-shape.
     geo_bounding_box query
     Finds documents with geo-points that fall into the specified rectangle.
     geo_distance query
     Finds document with geo-points within the specified distance of a central point.
     geo_polygon query
     Find documents with geo-points within the specified polygon.
     GeoShape Queryedit
     See Geo Shape Query

     Note: the geo_shape type uses Spatial4J and JTS, both of which are optional dependencies. Consequently you must add Spatial4J and JTS to your classpath in order to use this type:

     <dependency>
     <groupId>org.locationtech.spatial4j</groupId>
     <artifactId>spatial4j</artifactId>
     <version>0.6</version>
     </dependency>

     <dependency>
     <groupId>com.vividsolutions</groupId>
     <artifactId>jts</artifactId>
     <version>1.13</version>
     <exclusions>
     <exclusion>
     <groupId>xerces</groupId>
     <artifactId>xercesImpl</artifactId>
     </exclusion>
     </exclusions>
     </dependency>
     * @param s
     * @param multiPointBuilder
     * @return
     */
    public static QueryBuilder geoShapeQuery(String s, MultiPointBuilder multiPointBuilder){
        List<Coordinate> points = new ArrayList<Coordinate>();
        points.add(new Coordinate(0, 0));
        points.add(new Coordinate(0, 10));
        points.add(new Coordinate(10, 10));
        points.add(new Coordinate(10, 0));
        points.add(new Coordinate(0, 0));

        QueryBuilder qb = geoShapeQuery(
                "pin.location",
                ShapeBuilders.newMultiPoint(points));//.newMultiPoint(points).relation(ShapeRelation.WITHIN);
        return qb;
    }

    /**
     * Specialized queriesedit
     This group contains queries which do not fit into the other groups:
     more_like_this query
     This query finds documents which are similar to the specified text, document, or collection of documents.
     script query
     This query allows a script to act as a filter. Also see the function_score query.
     percolate query
     This query finds percolator queries based on documents.
     * @param fields
     * @param texts
     * @param items
     * @return
     */
    public static QueryBuilder  moreLikeThisQuery(String[] fields, String[] texts, MultiSearchResponse.Item[] items){
        /*String[] fields = {"name.first", "name.last"};
        String[] texts = {"text like this one"};
        MultiSearchResponse.Item[] items = null;*/
        QueryBuilder qb = moreLikeThisQuery(fields, texts, items);
        return qb;
    }

    /**
     * Span queries are low-level positional queries which provide expert control over the order and proximity of the specified terms. These are typically used to implement very specific queries on legal documents or patents.
     Span queries cannot be mixed with non-span queries (with the exception of the span_multi query).
     The queries in this group are:
     span_term query
     The equivalent of the term query but for use with other span queries.
     span_multi query
     Wraps a term, range, prefix, wildcard, regexp, or fuzzy query.
     span_first query
     Accepts another span query whose matches must appear within the first N positions of the field.
     span_near query
     Accepts multiple span queries whose matches must be within the specified distance of each other, and possibly in the same order.
     span_or query
     Combines multiple span queries — returns documents which match any of the specified queries.
     span_not query
     Wraps another span query, and excludes any documents which match that query.
     span_containing query
     Accepts a list of span queries, but only returns those spans which also match a second span query.
     span_within query
     The result from a single span query is returned as long is its span falls within the spans returned by a list of other span queries.
     * @param fielname
     * @param keywords
     * @return
     */
    public static QueryBuilder spanTermQuery(String fielname, String keywords){
        QueryBuilder qb = spanTermQuery(fielname, keywords);
        return qb;
    }
}
