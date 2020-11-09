package mapreduce.hotEntity_step1;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.google.common.collect.Iterables;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import util.StringUtil;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class HotEntityReducer_step1 extends Reducer<Text, LongWritable, Text, LongWritable> {

    private static final String[] PATTERNS = {
            "(((我?[想要])?看)|播放|点播)?(?<target>.*)导演的(电影|电视剧)",
            "(((我?[想要])?看)|播放|点播)?(?<target>.*?)[参出]?演的(电影|电视剧)",
            "(((我?[想要])?看)|播放|点播)?(?<target>.*)编剧的(电影|电视剧)",
            "(((我[想要])?听)|(播放)|(点播))?(?<target>.*?)唱?的歌曲?",
            "(((我?[想要])?看)|播放|点播)?(?<target>.*)的(电影|电视剧)",
            "(((我?[想要])?看)|播放|点播)?(电影|电视剧|动画片|综艺)(?<target>.*)",
            "(((我?[想要])?听)|播放|点播)?歌曲(?<target>.*)",
            "(下载|安装)(?<target>.*)",
            "切换到(?<target>.*)(频道|台)",
            "切换到(?<target>.*)",
            "(?<target>.*)((频道)|(台))",
            "打开(?<target>.*)",
            "^(我?[想要])?听(?<target>.*)",
            "^(我?[想要])?看(?<target>.*)"
    };

    private static final String[][] LABELS = {
            {"director"},   // 有director就+1
            {"actor"},  // 有actor就+1
            {"writer"}, // 有writer就+1
            {"singer"}, // 有singer就+1
            {"figure"}, // score>=0.2
            {"film"},   // label等于就+1
            {"song"},   // label等于就+1
            {"appname"},    // label等于就+1
            {"station"},    // label等于就+1
            {"station"},    // label等于就+1
            {"station"},    // label等于就+1
            {"station", "appname"}, // 按照优先级返回
            {"song", "chradio_album", "chradio_radio","chradio_program"}, // 按照优先级返回
            {"station", "film", "figure", "role"} //film>figure
    };

    private static final String[] EXCEPT = {"电影", "电视剧", "动画片", "动画", "电视", "综艺", "综艺节目", "直播", "歌曲", "音乐"};

    private static final ArangoDatabase DB = new ArangoDB.Builder().host("10.66.188.17", 8529).user("developer").password("changhongSSC20190813").build().db("knowledge-graph-test");

    private static boolean isSucceed = false;

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        isSucceed = false;

        String queryText = key.toString();
        int count = Iterables.size(values);

        // 统计实体
        String entityCal = "";
        // 搜库实体
        String entity = "";
        int matchRule = -1;
        String puncEx2 = "[(\\p{P}|\\p{S}|\\p{Z}|丨)&&[^+\\-~:：./%°℃]]";
        String puncNotBetweenDigitEx = "((?<![\\d])([/~\\-:：.+]))|(([/~\\-:：.+])(?![\\d]))";
        for(int i = 0; i < PATTERNS.length; i++){
            Pattern r = Pattern.compile(PATTERNS[i]);
            Matcher m = r.matcher(queryText);
            if(m.find()){
                // 匹配规则
                entityCal = m.group("target");
                // 去除特殊符号、英文转大写，阿拉伯数字转中文
                entity = StringUtil.arabicNumeralToChinese(entityCal.replaceAll(puncEx2, "").replaceAll(puncNotBetweenDigitEx, "").toUpperCase());
                // 匹配的第几条规则
                matchRule = i;
                break;
            }
        }

        // 去除停用词
        if (StringUtil.isEmpty(entity) || Arrays.asList(EXCEPT).contains(entity)) return;
        try{
            String query1 = "FOR c IN entity FILTER \"" + entity + "\" in c.formatNames AND c.status==\"active\" RETURN c";
            String query2 = "FOR c IN entity FILTER (\"" + entity + "\" in c.formatNames OR \"" + entity + "\" in c.formatNames_del) AND c.status!=\"quit\" RETURN c";
            ArangoCursor<JSONObject> cursor = DB.query(query1, null, null, JSONObject.class);
            if(!cursor.hasNext()) cursor = DB.query(query2, null, null, JSONObject.class);
            if(!cursor.hasNext()) return;
            JSONArray jsonArr = new JSONArray();
            cursor.forEachRemaining(jsonArr::add);

            // 1-4个规则对应单一profession，只要profession_active有，则+1
            if(matchRule != -1 && matchRule <= 3){
                String target = LABELS[matchRule][0];
                for(int i = 0; i < jsonArr.size(); i++){
                    if(jsonArr.getJSONObject(i).containsKey("profession_active")){
                        JSONArray proArr = jsonArr.getJSONObject(i).getJSONArray("profession_active");
                        for(int j = 0; j < proArr.size(); j++){
                            if(proArr.getJSONObject(j).getString("profession").equals(target)){
                                context.write(new Text(target+"|"+entityCal), new LongWritable(count));
                                context.write(new Text("figure|"+entityCal), new LongWritable(count));
                                return;
                            }
                        }
                    }
                }
            }

            // 第5个规则对应figure，根据分数判断，写入该figure所有>=0.2分数的profession，且写入一个figure
            if(matchRule == 4){
                figureProcess(context, count, entityCal, jsonArr);
            }

            // 第6-11个规则，对应单个label，只要有这个label就+1
            if(matchRule >= 5 && matchRule <= 10){
                String target = LABELS[matchRule][0];
                for(int i = 0; i < jsonArr.size(); i++){
                    if(jsonArr.getJSONObject(i).getString("label").equals(target)){
                        context.write(new Text(target+"|"+entityCal), new LongWritable(count));
                        return;
                    }
                }
            }

            Set<String> labelSet = new HashSet<>();
            for(int i = 0; i < jsonArr.size(); i++){
                labelSet.add(jsonArr.getJSONObject(i).getString("label"));
            }

            // 第12、13个规则，对应多个label，按优先级，高的+1
            if(matchRule == 11 || matchRule == 12){
                String[] target = LABELS[matchRule];
                for(String s : target){
                    if(labelSet.contains(s)){
                        context.write(new Text(s+"|"+entityCal), new LongWritable(count));
                        return;
                    }
                }
            }

            // 第14个规则，对应多个label，并且包含figure，先按优先级，如果是figure再单独处理
            if(matchRule == 13){
                String[] target = LABELS[matchRule];
                for(int i = 0; i < target.length; i++){
                    if((i <= 1 || i == 3) && labelSet.contains(target[i])){
//                        System.out.println();
//                        System.out.println("<<<<<<<<<<<<<<<< " + queryText + ">>>>>>>>>>>>>>>");
//                        System.out.println("<<<<<<<<<<<<<<<< " + entity + ">>>>>>>>>>>>>>>");
//                        System.out.println("<<<<<<<<<<<<<<<< " + target[i] + ">>>>>>>>>>>>>>>");
//                        System.out.println();
                        context.write(new Text(target[i]+"|"+entityCal), new LongWritable(count));
                        return;
                    }
                    if(i == 2 && labelSet.contains(target[i])){
                        figureProcess(context, count, entityCal, jsonArr);
                        if(isSucceed) return;
                    }
                }
            }


        } catch (Exception e){
            log.error("arango访问失败次数+1：" + e.getMessage());
        }

    }

    private void figureProcess(Context context, int count, String entityCal, JSONArray jsonArr) throws IOException, InterruptedException {
        boolean flag = false;
        List<String> alreadyIn = new ArrayList<>();
        for(int i = 0; i < jsonArr.size(); i++){
            if(jsonArr.getJSONObject(i).containsKey("profession_active")){
                JSONArray proArr = jsonArr.getJSONObject(i).getJSONArray("profession_active");
                for(int j = 0; j < proArr.size(); j++){
                    JSONObject proAct = proArr.getJSONObject(j);
                    String target = proAct.getString("profession");
                    if(proAct.getDouble("score") >= 0.2 && !alreadyIn.contains(target)){
                        // "我想听"就不能是singer以外的，"我想看"就不能是singer
                        if(!target.equals("singer")){
                            context.write(new Text(target+"|"+entityCal), new LongWritable(count));
                            isSucceed = true;
                        }
                        alreadyIn.add(target);
                        if(!flag){
                            context.write(new Text("figure|"+entityCal), new LongWritable(count));
                            flag = true;
                            isSucceed = true;
                        }
                    }
                }
            }
        }
    }

}



