package cn.leancloud.sample.testcase;

import cn.leancloud.LCException;
import cn.leancloud.LeanCloud;
import cn.leancloud.sample.DemoBaseActivity;
import cn.leancloud.sample.Student;
import cn.leancloud.core.AppConfiguration;

/**
 * Created by fengjunwen on 2018/5/10.
 */

public class OtherDemoActivity extends DemoBaseActivity {
//  public void testGetSereverDate() throws LCException {
//    Date date = AVOSCloud.getServerDate();
//    log("服务器时间：" + date);
//  }

  public void testConfigNetworkTimeout() throws LCException {
    // 得放到 Application 里
    LeanCloud.setNetworkTimeout(10);
    try {
      Student student = getFirstStudent();
      log("student:" + prettyJSON(student));
    } catch (LCException e) {
      log("因为设置了网络超时为 10 毫秒，所以超时了，e:" + e.getMessage());
    }
    LeanCloud.setNetworkTimeout(AppConfiguration.DEFAULT_NETWORK_TIMEOUT);
  }

}
