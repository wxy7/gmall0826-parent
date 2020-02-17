package com.atguigu.gmall0826.publisher.service.impl;

import com.atguigu.gmall0826.common.constants.GmallConstant;
import com.atguigu.gmall0826.publisher.mapper.DauMapper;
import com.atguigu.gmall0826.publisher.mapper.OrderMapper;
import com.atguigu.gmall0826.publisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * author : wuyan
 * create : 2020-02-10 18:30
 * desc :
 */
@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;

    @Autowired
    JestClient jestClient;

    @Override
    public Long getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map<String, Long> getDauTotalHour(String date) {
        List<Map> listMap = dauMapper.selectDauTotalHours(date);
        HashMap<String, Long> dauMap = new HashMap();
        for (Map map : listMap) {
            String logHour = (String)map.get("logHour");
            Long count = (Long)map.get("count");
            dauMap.put(logHour, count);
        }
        return dauMap;
    }

    @Override
    public Double getOrderAmount(String date) {
        return orderMapper.selectOrderAmount(date);
    }

    @Override
    public Map<String, Double> getOrderAmountHour(String date) {
        List<Map> list = orderMapper.selectOrderAmountHour(date);
        HashMap<String ,Double> hashMap = new HashMap<>();
        for (Map map : list) {
            String create_hour = (String) map.get("create_hour");
            System.out.println("create_hour:" +  create_hour);
            Double order_amount = (Double) map.get("order_amount");
            System.out.println("order_amount:" +  order_amount);
            hashMap.put(create_hour, order_amount);
        }
        return hashMap;
    }

    /***
     *
     * @param date
     * @param keyword
     * @param pageNo
     * @param pageSize
     * 查询语句：
     * GET gmall0826_sale_detail/_search
     * {
     *   "query": {
     *     "bool": {
     *       "filter": {
     *         "term": {
     *           "dt": "2020-02-17"
     *         }
     *       },
     *       "must": [
     *         {
     *           "match": {
     *             "sku_name": {
     *               "query": "小米手机",
     *               "operator": "and"
     *             }
     *           }
     *         }
     *       ]
     *     }
     *   },
     *   "aggs": {
     *     "groupby_age": {
     *       "terms": {
     *         "field": "user_age",
     *         "size": 120
     *       }
     *     },
     *     "groupby_gender":{
     *       "terms": {
     *         "field": "user_gender",
     *         "size": 20
     *       }
     *     }
     *   },
     *   "from": 0,
     *   "size": 10
     * }
     * @return
     */
    @Override
    public Map getSaleDetail(String date, String keyword, int pageNo, int pageSize) {

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();//相当于最外面那层{query，aggs,from, size}


        //具体过滤条件
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();//相当于查询语句的bool
        boolQueryBuilder.filter(new TermQueryBuilder("dt", date));//相当于查询语句的filer
        boolQueryBuilder
                .must(new MatchQueryBuilder("sku_name", keyword).operator(MatchQueryBuilder.Operator.AND));//相当于查询语句的must
        //过滤->query
        searchSourceBuilder.query(boolQueryBuilder);//.query相当于查询语句的第一个query模块


        //具体分组聚合条件
        //.terms(name)-》给分组取得名字   .field("user_age")-》按user_age这个字段分组   .size(120)-》最多分成120组
        TermsBuilder ageBuilder = AggregationBuilders.terms("groupby_age").field("user_age").size(120);//年龄分组聚合
        TermsBuilder genderBuilder = AggregationBuilders.terms("groupby_gender").field("user_gender").size(10);//性别分组聚合
        //聚合->aggs
        searchSourceBuilder.aggregation(ageBuilder);
        searchSourceBuilder.aggregation(genderBuilder);

        //分页->from size
        int rowNo = (pageNo - 1) * pageSize;
        searchSourceBuilder.from(rowNo); //行号
        searchSourceBuilder.size(pageSize);
        System.out.println("================pageSize=" + pageSize );


        //Builder里放SearchSourceBuilder的对象的string
        //整个查询语句 + 库名 + 表名
        Search search = new Search
                .Builder(searchSourceBuilder.toString())//searchSourceBuilder.toString()相当于证据查询语句
                .addIndex(GmallConstant.ES_ALERT_SALE)//index名=库名
                .addType("_doc").build();//默认的表名，实际无作用

        HashMap map = new HashMap();//明细 总数 年龄聚合结果  性别聚合结果

        //查询结果
        try {
            SearchResult searchResult = jestClient.execute(search);

            //明细数据
            List<Map> detailList = new ArrayList<>();
            List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
            for (SearchResult.Hit<Map, Void> hit : hits) {
                detailList.add(hit.source);
            }

            //总条数
            Long total = 0L;
            total = searchResult.getTotal();
            System.out.println("===================total=" + total);

            //年龄的聚合结果
            HashMap ageAggMap = new HashMap();
            List<TermsAggregation.Entry> ageEntryList = searchResult.getAggregations().getTermsAggregation("groupby_age").getBuckets();
            for (TermsAggregation.Entry entry : ageEntryList) {
                ageAggMap.put(entry.getKey(), entry.getCount());
            }

            //性别的聚合结果
            HashMap genderAggMap = new HashMap();
            List<TermsAggregation.Entry> genderEntryList = searchResult.getAggregations().getTermsAggregation("groupby_gender").getBuckets();
            for (TermsAggregation.Entry entry : genderEntryList) {
                genderAggMap.put(entry.getKey(), entry.getCount());
            }

            map.put("detailList", detailList);
            map.put("total", total);
            map.put("ageAggMap", ageAggMap);
            map.put("genderAggMap", genderAggMap);



        } catch (IOException e) {
            e.printStackTrace();
        }

        return map;
    }
}
