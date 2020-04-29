import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

@Controller
@RequestMapping("/hive")
public class HiveController {
    private static final Logger logger = LoggerFactory.getLogger(HiveController.class);

    @Autowired
    @Qualifier("hiveJdbcTemplate")
    private JdbcTemplate hiveJdbcTemplate;

    @RequestMapping("/create")
    public ModelAndView create() {

        StringBuffer sql = new StringBuffer("create table IF NOT EXISTS ");
        sql.append("HIVE_TEST");
        sql.append("(KEY INT, VALUE STRING)");
        sql.append ("PARTITIONED BY (CTIME DATE)"); // partitioned storage
        sql.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' "); // 定义分隔符
        sql.append ("STORED AS TEXTFILE"); // as text storage

        logger.info(sql.toString());
        hiveJdbcTemplate.execute(sql.toString());

        return new ModelAndView("index");

    }

    @RequestMapping("/insert")
    public String insert() {
        hiveJdbcTemplate.execute("insert into hive_test(key, value) values('Neo','Chen')");
        return "Done";
    }

    @RequestMapping("/select")
    public String select() {
        String sql = "select * from HIVE_TEST";
        List<Map<String, Object>> rows = hiveJdbcTemplate.queryForList(sql);
        Iterator<Map<String, Object>> it = rows.iterator();
        while (it.hasNext()) {
            Map<String, Object> row = it.next();
            System.out.println(String.format("%s\t%s", row.get("key"), row.get("value")));
        }
        return "Done";
    }

    @RequestMapping("/delete")
    public String delete() {
        StringBuffer sql = new StringBuffer("DROP TABLE IF EXISTS ");
        sql.append("HIVE_TEST");
        logger.info(sql.toString());
        hiveJdbcTemplate.execute(sql.toString());
        return "Done";
    }
}