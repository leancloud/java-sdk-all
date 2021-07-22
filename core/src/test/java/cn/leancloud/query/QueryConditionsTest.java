package cn.leancloud.query;

import cn.leancloud.LCFriendship;
import cn.leancloud.json.JSON;
import junit.framework.TestCase;

public class QueryConditionsTest extends TestCase {
    public void testJsonParams() throws Exception {
        int offset = 10;
        int limit = 20;
        QueryConditions conditions = new QueryConditions();
        conditions.whereEqualTo(LCFriendship.ATTR_FRIEND_STATUS, true);
        if (offset > 0) {
            conditions.setSkip(offset);
        }
        if (limit > 0) {
            conditions.setLimit(limit);
        }
        System.out.println(conditions.assembleJsonParam());
        System.out.println(conditions.assembleParameters());
        System.out.println(conditions.getParameters());
        System.out.println(JSON.toJSONString(conditions.assembleJsonParam()));
    }
}
