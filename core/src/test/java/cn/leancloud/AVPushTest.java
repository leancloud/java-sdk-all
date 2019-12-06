package cn.leancloud;

import com.alibaba.fastjson.JSONObject;
import io.reactivex.Observer;
import io.reactivex.disposables.Disposable;
import junit.framework.TestCase;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class AVPushTest extends TestCase {
  private boolean testSucceed;

  public AVPushTest(String name) {
    super(name);
    Configure.initializeRuntime();
  }

  public void testSimplePush() throws Exception {
    AVPush push = new AVPush();
    push.setMessage("test from unittest");
    push.send();
  }

  public void testPushFlowControl() throws Exception {
    AVPush push = new AVPush();
    Map<String, Object> pushData = new HashMap<>();
    pushData.put("alert", "push message to android device directly");
    push.setPushToAndroid(true);
    push.setData(pushData);
    push.setFlowControl( 200);
    assertEquals(push.getFlowControl(), 1000);

    final CountDownLatch latch = new CountDownLatch(1);
    testSucceed = false;
    push.sendInBackground().subscribe(new Observer<JSONObject>() {
      @Override
      public void onSubscribe(Disposable d) {
      }

      @Override
      public void onNext(JSONObject jsonObject) {
        System.out.println("推送成功" + jsonObject);
        testSucceed = true;
        latch.countDown();
      }

      @Override
      public void onError(Throwable e) {
        e.printStackTrace();
        System.out.println("推送失败，错误信息：" + e.getMessage());
        latch.countDown();
      }

      @Override
      public void onComplete() {
      }
    });
    latch.await();
    assertTrue(testSucceed);
  }

  public void testPushTargetWithData() throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);
    testSucceed = false;
    AVPush push = new AVPush();
    Map<String, Object> pushData = new HashMap<>();
    pushData.put("alert", "push message to android device directly");
    push.setPushToAndroid(true);
    push.setData(pushData);
    push.sendInBackground().subscribe(new Observer<JSONObject>() {
      @Override
      public void onSubscribe(Disposable d) {
      }

      @Override
      public void onNext(JSONObject jsonObject) {
        System.out.println("推送成功" + jsonObject);
        testSucceed = true;
        latch.countDown();
      }

      @Override
      public void onError(Throwable e) {
        e.printStackTrace();
        System.out.println("推送失败，错误信息：" + e.getMessage());
        latch.countDown();
      }

      @Override
      public void onComplete() {
      }
    });
    latch.await();
    assertTrue(testSucceed);
  }
}
