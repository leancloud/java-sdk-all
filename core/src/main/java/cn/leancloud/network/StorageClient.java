package cn.leancloud.network;

import cn.leancloud.*;
import cn.leancloud.query.AVQueryResult;
import cn.leancloud.service.APIService;
import cn.leancloud.types.AVDate;
import cn.leancloud.types.AVNull;
import cn.leancloud.upload.FileUploadToken;
import cn.leancloud.utils.LogUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.reactivex.Observable;
import io.reactivex.Scheduler;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class StorageClient {
  private static AVLogger LOGGER = LogUtil.getLogger(StorageClient.class);

  private APIService apiService = null;
  private boolean asynchronized = false;
  private PaasClient.SchedulerCreator defaultCreator = null;
  public StorageClient(APIService apiService, boolean asyncRequest, PaasClient.SchedulerCreator observerSchedulerCreator) {
    this.apiService = apiService;
    this.asynchronized = asyncRequest;
    this.defaultCreator = observerSchedulerCreator;
  }

  private Observable wrappObservable(Observable observable) {
    if (null == observable) {
      return null;
    }
    if (asynchronized) {
      observable = observable.subscribeOn(Schedulers.io());
    }
    if (null != defaultCreator) {
      observable = observable.observeOn(defaultCreator.create());
    }
    return observable;
  }
  private Observable wrappObservableInBackground(Observable observable) {
    if (null == observable) {
      return null;
    }
    Scheduler scheduler = Schedulers.io();
    if (asynchronized) {
      observable = observable.subscribeOn(scheduler);
    }
    if (null != defaultCreator) {
      observable = observable.observeOn(scheduler);
    }
    return observable;
  }

  public Observable<AVDate> getServerTime() {
    Observable<AVDate> date = wrappObservable(apiService.currentTimeMillis());
    return date;
  }

  public Observable<? extends AVObject> fetchObject(final String className, String objectId) {
    Observable<AVObject> object = wrappObservable(apiService.fetchObject(className, objectId));
    return object.map(new Function<AVObject, AVObject>() {
              public AVObject apply(AVObject avObject) throws Exception {
                return Transformer.transform(avObject, className);
              }
            });
  }

  public Observable<List<AVObject>> queryObjects(final String className, Map<String, String> query) {
    return wrappObservable(apiService.queryObjects(className, query)).map(new Function<AVQueryResult, List<AVObject>>() {
      public List<AVObject> apply(AVQueryResult o) throws Exception {
        LOGGER.d("invoke within StorageClient.queryObjects(). resultSize:" + ((null != o.getResults())? o.getResults().size(): 0));
        return o.getResults();
      }
    });
  }

  public Observable<Integer> queryCount(final String className, Map<String, String> query) {
    return wrappObservable(apiService.queryObjects(className, query).map(new Function<AVQueryResult, Integer>() {
      public Integer apply(AVQueryResult o) throws Exception {
        LOGGER.d("invoke within StorageClient.queryCount(). result:" + o + ", return:" + o.getCount());
        return o.getCount();
      }
    }));
  }

  public Observable<AVNull> deleteObject(final String className, String objectId) {
    return wrappObservable(apiService.deleteObject(className, objectId));
  }

  public Observable<? extends AVObject> createObject(final String className, JSONObject data, boolean fetchFlag) {
    Observable<AVObject> object = wrappObservable(apiService.createObject(className, data, fetchFlag));
    return object.map(new Function<AVObject, AVObject>() {
      public AVObject apply(AVObject avObject) {
        LOGGER.d(avObject.toString());
        return Transformer.transform(avObject, className);
      }
    });
  }

  public Observable<? extends AVObject> saveObject(final String className, String objectId, JSONObject data, boolean fetchFlag) {
    Observable<AVObject> object = wrappObservable(apiService.updateObject(className, objectId, data, fetchFlag));
    return object.map(new Function<AVObject, AVObject>() {
      public AVObject apply(AVObject avObject) {
        LOGGER.d("saveObject finished. intermediaObj=" + avObject.toString() + ", convert to " + className);
        return Transformer.transform(avObject, className);
      }
    });
  }

  public Observable<AVFile> fetchFile(String objectId) {
    Observable<AVFile> object = wrappObservable(apiService.fetchFile(objectId));
    return object.map(new Function<AVFile, AVFile>() {
      public AVFile apply(AVFile avFile) throws Exception {
        avFile.setClassName(AVFile.CLASS_NAME);
        return avFile;
      }
    });
  }

  public Observable<FileUploadToken> newUploadToken(JSONObject fileData) {
    return wrappObservableInBackground(apiService.createUploadToken(fileData));
  }

  public void fileCallback(JSONObject result) throws IOException {
    apiService.fileCallback(result).execute();
    return;
  }

  public Observable<JSONArray> batchSave(JSONObject parameter) {
    // resposne is:
    // [{"success":{"updatedAt":"2018-03-30T06:21:08.052Z","objectId":"5abd026d9f54540038791715"}},
    //  {"success":{"updatedAt":"2018-03-30T06:21:08.092Z","objectId":"5abd026d9f54540038791715"}},
    //  {"success":{"updatedAt":"2018-03-30T06:21:08.106Z","objectId":"5abd026d9f54540038791715"}}]
    Observable<JSONArray> result = wrappObservable(apiService.batchCreate(parameter));
    return result;
  }

  public Observable<JSONObject> batchUpdate(JSONObject parameter) {
    // response is:
    // {"5abd026d9f54540038791715":{"updatedAt":"2018-03-30T06:21:46.084Z","objectId":"5abd026d9f54540038791715"}}
    Observable<JSONObject> result = wrappObservable(apiService.batchUpdate(parameter));
    return result;
  }

  public Observable<AVUser> signUp(JSONObject data) {
    return wrappObservable(apiService.signup(data));
  }

  public <T extends AVUser> Observable<T> logIn(JSONObject data, final Class clazz) {
    Observable<JSONObject> object = wrappObservable(apiService.login(data));
    return object.map(new Function<JSONObject, T>() {
      public T apply(JSONObject object) throws Exception {
        LOGGER.d("convert JSONObject to target Class:" + clazz.getCanonicalName());
        T result = (T) JSON.parseObject(object.toJSONString(), clazz);
        LOGGER.d("result:" + result);
        return result;
      }
    });
  }
}
