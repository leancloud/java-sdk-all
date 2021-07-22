package cn.leancloud;

import cn.leancloud.annotation.LCClassName;
import cn.leancloud.cache.PersistenceUtil;
import cn.leancloud.callback.LCCallback;
import cn.leancloud.callback.FollowersAndFolloweesCallback;
import cn.leancloud.core.AppConfiguration;
import cn.leancloud.core.PaasClient;
import cn.leancloud.core.StorageClient;
import cn.leancloud.gson.ObjectDeserializer;
import cn.leancloud.ops.Utils;
import cn.leancloud.query.QueryConditions;
import cn.leancloud.sms.LCSMS;
import cn.leancloud.sms.LCSMSOption;
import cn.leancloud.types.LCNull;
import cn.leancloud.utils.LCUtils;
import cn.leancloud.utils.ErrorUtils;
import cn.leancloud.utils.StringUtil;
import cn.leancloud.json.JSON;
import cn.leancloud.json.JSONObject;
import io.reactivex.Observable;
import io.reactivex.Observer;
import io.reactivex.annotations.NonNull;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Function;

import java.io.File;
import java.util.*;

// TODO: need transfer Anonymous User/Common User

@LCClassName("_User")
public class LCUser extends LCObject {
  public static final String ATTR_USERNAME = "username";
  private static final String ATTR_PASSWORD = "password";
  public static final String ATTR_EMAIL = "email";
  public static final String ATTR_MOBILEPHONE = "mobilePhoneNumber";
  private static final String ATTR_SMSCODE = "smsCode";
  private static final String ATTR_MOBILEPHONE_VERIFIED = "mobilePhoneVerified";
  public static final String ATTR_SESSION_TOKEN = "sessionToken";

  private static final String PARAM_ATTR_FRIENDSHIP = "friendship";

  private static final String AUTHDATA_TAG = "authData";
  private static final String AUTHDATA_PLATFORM_ANONYMOUS = "anonymous";

  private static final String AUTHDATA_ATTR_UNIONID = "unionid";
  private static final String AUTHDATA_ATTR_UNIONID_PLATFORM = "platform";
  private static final String AUTHDATA_ATTR_MAIN_ACCOUNT = "main_account";

  private static final String ILLEGALARGUMENT_MSG_FORMAT = "illegal parameter. %s must not null/empty.";

  public static final String CLASS_NAME = "_User";
  public static final String FOLLOWER_TAG = "follower";
  public static final String FOLLOWEE_TAG = "followee";

  private static Class<? extends LCUser> subClazz = null;

  public enum SNS_PLATFORM {
    FACEBOOK("facebook"), TWITTER("twitter"), QQ("qq"), WEIBO("weibo"), WECHAT("weixin");
    SNS_PLATFORM(String name) {
      this.name = name;
    }
    private String name;
    public String getName() {
      return this.name;
    }
  }

  /**
   * constructor
   */
  public LCUser() {
    super(CLASS_NAME);
  }

  /**
   * 获取当前登录用户
   *
   * @return current user.
   *
   * Notice: you SHOULDN'T invoke this method to get current user in lean engine.
   */
  public static LCUser currentUser() {
    return getCurrentUser();
  }

  /**
   * get user email.
   * @return user email.
   */
  public String getEmail() {
    return (String) get(ATTR_EMAIL);
  }

  /**
   * set user email
   *
   * @param email user email.
   */
  public void setEmail(String email) {
    put(ATTR_EMAIL, email);
  }

  /**
   * get user name.
   *
   * @return user name
   */
  public String getUsername() {
    return (String) get(ATTR_USERNAME);
  }

  /**
   * set user name.
   *
   * @param name username
   */
  public void setUsername(String name) {
    put(ATTR_USERNAME, name);
  }

  /**
   * get user password.
   * @return user password.
   */
  public String getPassword() {
    return (String) get(ATTR_PASSWORD);
  }

  /**
   * set user password.
   *
   * @param password user password.
   */
  public void setPassword(String password) {
    put(ATTR_PASSWORD, password);
  }

  /**
   * get user mobilephone.
   *
   * @return user mobilephone number.
   */
  public String getMobilePhoneNumber() {
    return (String) get(ATTR_MOBILEPHONE);
  }

  /**
   * set user mobilephone.
   *
   * @param mobile user mobilephone number.
   */
  public void setMobilePhoneNumber(String mobile) {
    put(ATTR_MOBILEPHONE, mobile);
  }

  /**
   * whether user's mobilephone is verified or not.
   *
   * @return flag to indicate user's mobilephone is verified or not
   */
  public boolean isMobilePhoneVerified() {
    return getBoolean(ATTR_MOBILEPHONE_VERIFIED);
  }

  /**
   * get user session token.
   * if user not login, session token is null.
   *
   * @return user session token, null if not login.
   */
  public String getSessionToken() {
    return (String)get(ATTR_SESSION_TOKEN);
  }

  /**
   * not use it!
   * @param token user token.
   */
  public void internalChangeSessionToken(String token) {
    getServerData().put(ATTR_SESSION_TOKEN, token);
  }

  /**
   * whether user is authenticated or not.
   * @return flag to indicate user is authenticated or not.
   */
  public boolean isAuthenticated() {
    // TODO: need to support thirdparty login.
    String sessionToken = getSessionToken();
    return !StringUtil.isEmpty(sessionToken);
  }

  private void updateCurrentUserCache() {
    String sessionToken = getSessionToken();
    LCUser currentUser = LCUser.currentUser();
    if (null != currentUser && !StringUtil.isEmpty(currentUser.getObjectId())
            && currentUser.getObjectId().equals(getObjectId()) && !StringUtil.isEmpty(sessionToken)) {
      changeCurrentUser(this, true);
    }
  }

  @Override
  protected void onSaveSuccess() {
    super.onSaveSuccess();
    updateCurrentUserCache();
  }

  @Override
  protected void onSaveFailure() {
    super.onSaveFailure();
  }

  @Override
  protected void onDataSynchronized() {
    super.onDataSynchronized();
    updateCurrentUserCache();
  }

  /**
   * Whether is anonymous or not.
   * @return flag to indicate current user is anonymous or not.
   */
  public boolean isAnonymous() {
    JSONObject existedAuthData = this.getJSONObject(AUTHDATA_TAG);
    if (existedAuthData != null) {
      if (existedAuthData.size() == 1 && existedAuthData.containsKey(AUTHDATA_PLATFORM_ANONYMOUS)) {
        return true;
      }
    }
    return false;
  }

  /**
   * sign up(blocking).
   */
  public void signUp() {
    signUpInBackground().blockingSubscribe();
  }

  /**
   * sign up in background.
   * @return observable instance.
   */
  public Observable<LCUser> signUpInBackground() {
    JSONObject paramData = generateChangedParam();
    logger.d("signup param: " + paramData.toJSONString());
    return PaasClient.getStorageClient().signUp(paramData).map(new Function<LCUser, LCUser>() {
      @Override
      public LCUser apply(LCUser avUser) throws Exception {
        LCUser.this.mergeRawData(avUser, true);
        LCUser.this.onSaveSuccess();
        return LCUser.this;
      }
    });
  }

  /**
   * signUpOrLoginByMobilePhone
   *
   * @param mobilePhoneNumber mobile phone
   * @param smsCode sms code
   * @return user instance.
   */
  public static LCUser signUpOrLoginByMobilePhone(String mobilePhoneNumber, String smsCode) {
    return signUpOrLoginByMobilePhone(mobilePhoneNumber, smsCode, internalUserClazz());
  }

  /**
   * signUpOrLoginByMobilePhone
   * @param mobilePhoneNumber mobile phone number
   * @param smsCode sms code
   * @param clazz class name
   * @param <T> template type.
   * @return user instance.
   */
  public static <T extends LCUser> T signUpOrLoginByMobilePhone(String mobilePhoneNumber, String smsCode, Class<T> clazz) {
    return signUpOrLoginByMobilePhoneInBackground(mobilePhoneNumber, smsCode, clazz).blockingSingle();
  }

  /**
   * signUpOrLoginByMobilePhoneInBackground
   * @param mobilePhoneNumber mobile phone number.
   * @param smsCode sms code
   * @return observable instance.
   */
  public static Observable<? extends LCUser> signUpOrLoginByMobilePhoneInBackground(String mobilePhoneNumber, String smsCode) {
    return signUpOrLoginByMobilePhoneInBackground(mobilePhoneNumber, smsCode, internalUserClazz());
  }

  /**
   * signUpOrLoginByMobilePhoneInBackground
   *
   * @param mobilePhoneNumber mobile phone number
   * @param smsCode sms code
   * @param clazz class name
   * @param <T> template type.
   * @return observable instance.
   */
  public static <T extends LCUser> Observable<T> signUpOrLoginByMobilePhoneInBackground(String mobilePhoneNumber,
                                                                                        String smsCode, Class<T> clazz) {
    if (StringUtil.isEmpty(mobilePhoneNumber)) {
      return Observable.error(new IllegalArgumentException(String.format(ILLEGALARGUMENT_MSG_FORMAT, "mobilePhoneNumber")));
    }
    if (StringUtil.isEmpty(smsCode)) {
      return Observable.error(new IllegalArgumentException(String.format(ILLEGALARGUMENT_MSG_FORMAT, "smsCode")));
    }
    if (null == clazz) {
      return Observable.error(new IllegalArgumentException(String.format(ILLEGALARGUMENT_MSG_FORMAT, "clazz")));
    }
    Map<String, Object> params = createUserMap(null, null, null, mobilePhoneNumber, smsCode);
    JSONObject data = JSONObject.Builder.create(params);
    return PaasClient.getStorageClient().signUpOrLoginByMobilephone(data, clazz);
  }

  /**
   * logIn in background
   *
   * @param username username
   * @param password user password
   * @return observable instance.
   */
  public static Observable<? extends LCUser> logIn(String username, String password) {
    return logIn(username, password, internalUserClazz());
  }

  /**
   * login as anonymous user in background.
   * @return observable instance.
   */
  public static Observable<? extends LCUser> logInAnonymously() {
    String anonymousId = LCInstallation.getCurrentInstallation().getInstallationId();
    Map<String, Object> param = new HashMap<>();
    param.put("id", anonymousId);
    return loginWithAuthData(param, AUTHDATA_PLATFORM_ANONYMOUS);
  }

  /**
   * logIn in background
   *
   * @param username username
   * @param password user password
   * @param clazz user class name
   * @param <T> template type.
   * @return observable instance.
   */
  public static <T extends LCUser> Observable<T> logIn(String username, String password, final Class<T> clazz) {
    Map<String, Object> params = createUserMap(username, password, null, null, null);
    JSONObject data = JSONObject.Builder.create(params);
    return PaasClient.getStorageClient().logIn(data, clazz);
  }

  /**
   * logIn with mobile phone and password.
   * @param mobile mobile phone
   * @param password password
   * @return observable instance.
   */
  public static Observable<? extends LCUser> loginByMobilePhoneNumber(String mobile, String password) {
    return loginByMobilePhoneNumber(mobile, password, internalUserClazz());
  }

  /**
   * logIn with email and password
   * @param email email.
   * @param password password.
   * @return observable instance.
   */
  public static Observable<? extends LCUser> loginByEmail(String email, String password) {
    HashMap<String, Object> params = createUserMapAFAP(null, password, email, null, null);
    return PaasClient.getStorageClient().logIn(JSONObject.Builder.create(params), internalUserClazz());
  }

  /**
   * logIn with mobile phone and password.
   * @param mobile mobile phone.
   * @param password user password.
   * @param clazz user class.
   * @param <T> template type.
   * @return observable instance.
   */
  public static <T extends LCUser> Observable<T> loginByMobilePhoneNumber(String mobile, String password,
                                                                          final Class<T> clazz) {
    Map<String, Object> params = createUserMap(null, password, null, mobile, null);
    JSONObject data = JSONObject.Builder.create(params);
    return PaasClient.getStorageClient().logIn(data, clazz);
  }

  /**
   * logIn with mobile phone and sms code.
   * @param mobile mobile phone.
   * @param smsCode sms code.
   * @return observable instance.
   */
  public static Observable<? extends LCUser> loginBySMSCode(String mobile, String smsCode) {
    return loginBySMSCode(mobile, smsCode, internalUserClazz());
  }

  /**
   * logIn with mobile phone and sms code.
   * @param mobile mobile phone.
   * @param smsCode sms code.
   * @param clazz user class.
   * @param <T> template type.
   * @return observable instance.
   */
  public static <T extends LCUser> Observable<T> loginBySMSCode(String mobile, String smsCode, Class<T> clazz) {
    Map<String, Object> params = createUserMap(null, null, null, mobile, smsCode);
    JSONObject data = JSONObject.Builder.create(params);
    return PaasClient.getStorageClient().logIn(data, clazz);
  }

  private static Map<String, Object> createUserMap(String username, String password, String email,
                                                   String phoneNumber, String smsCode) {
    Map<String, Object> map = new HashMap<String, Object>();

    if (StringUtil.isEmpty(username) && StringUtil.isEmpty(phoneNumber)) {
      throw new IllegalArgumentException("Blank username and blank mobile phone number");
    }
    if (!StringUtil.isEmpty(username)) {
      map.put(ATTR_USERNAME, username);
    }
    if (!StringUtil.isEmpty(password)) {
      map.put(ATTR_PASSWORD, password);
    }
    if (!StringUtil.isEmpty(email)) {
      map.put(ATTR_EMAIL, email);
    }
    if (!StringUtil.isEmpty(phoneNumber)) {
      map.put(ATTR_MOBILEPHONE, phoneNumber);
    }
    if (!StringUtil.isEmpty(smsCode)) {
      map.put(ATTR_SMSCODE, smsCode);
    }
    return map;
  }

  private static HashMap<String, Object> createUserMapAFAP(String username, String password, String email,
                                                       String phoneNumber, String smsCode) {
    HashMap<String, Object> map = new HashMap<String, Object>();

    if (!StringUtil.isEmpty(username)) {
      map.put(ATTR_USERNAME, username);
    }
    if (!StringUtil.isEmpty(password)) {
      map.put(ATTR_PASSWORD, password);
    }
    if (!StringUtil.isEmpty(email)) {
      map.put(ATTR_EMAIL, email);
    }
    if (!StringUtil.isEmpty(phoneNumber)) {
      map.put(ATTR_MOBILEPHONE, phoneNumber);
    }
    if (!StringUtil.isEmpty(smsCode)) {
      map.put(ATTR_SMSCODE, smsCode);
    }
    return map;
  }

  /**
   * third-party login methods.
   */

  /**
   * login with auth data.
   * @param authData auth data.
   * @param platform platform string.
   * @return observable instance.
   */
  public static Observable<? extends LCUser> loginWithAuthData(final Map<String, Object> authData, final String platform) {
    return loginWithAuthData(internalUserClazz(), authData, platform);
  }

  /**
   * login with auth data.
   * @param authData auth data.
   * @param platform platform string.
   * @param unionId unionid.
   * @param unionIdPlatform unionid platform string.
   * @param asMainAccount flag to treat as main account.
   * @return observable instance.
   */
  public static Observable<? extends LCUser> loginWithAuthData(final Map<String, Object> authData, final String platform,
                                                               final String unionId, final String unionIdPlatform,
                                                               final boolean asMainAccount) {
    return loginWithAuthData(internalUserClazz(), authData, platform, unionId, unionIdPlatform, asMainAccount);
  }

  /**
   * login with auth data.
   * @param authData auth data.
   * @param platform platform string.
   * @param clazz user class name.
   * @param <T> template type.
   * @return observable instance.
   */
  public static <T extends LCUser> Observable<T> loginWithAuthData(final Class<T> clazz,
                                                                   final Map<String, Object> authData,
                                                                   final String platform) {
    if (null == clazz) {
      return Observable.error(new IllegalArgumentException(String.format(ILLEGALARGUMENT_MSG_FORMAT, "clazz")));
    }
    if (null == authData || authData.isEmpty()) {
      return Observable.error(new IllegalArgumentException(String.format(ILLEGALARGUMENT_MSG_FORMAT, "authData")));
    }
    if (StringUtil.isEmpty(platform)) {
      return Observable.error(new IllegalArgumentException(String.format(ILLEGALARGUMENT_MSG_FORMAT, "platform")));
    }
    Map<String, Object> data = new HashMap<String, Object>();
    Map<String, Object> authMap = new HashMap<String, Object>();
    authMap.put(platform, authData);
    data.put(AUTHDATA_TAG, authMap);
    JSONObject param = JSONObject.Builder.create(data);
    return PaasClient.getStorageClient().signUp(param).map(new Function<LCUser, T>() {
      @Override
      public T apply(LCUser avUser) throws Exception {
        T result = Transformer.transform(avUser, clazz);
        LCUser.changeCurrentUser(result, true);
        return result;
      }
    });
  }

  /**
   * login with auth data.
   * @param authData auth data.
   * @param platform platform string.
   * @param unionId unionid.
   * @param unionIdPlatform unionid platform string.
   * @param asMainAccount flag to treat as main account.
   * @param clazz user class name.
   * @param <T> template type.
   * @return observable instance.
   */
  public static <T extends LCUser> Observable<T> loginWithAuthData(final Class<T> clazz, final Map<String, Object> authData,
                                                                   final String platform, final String unionId,
                                                                   final String unionIdPlatform, final boolean asMainAccount) {
    if (StringUtil.isEmpty(unionId)) {
      return Observable.error(new IllegalArgumentException(String.format(ILLEGALARGUMENT_MSG_FORMAT, "unionId")));
    }
    if (StringUtil.isEmpty(unionIdPlatform)) {
      return Observable.error(new IllegalArgumentException(String.format(ILLEGALARGUMENT_MSG_FORMAT, "unionIdPlatform")));
    }
    if (null == authData || authData.isEmpty()) {
      return Observable.error(new IllegalArgumentException(String.format(ILLEGALARGUMENT_MSG_FORMAT, "authData")));
    }
    authData.put(AUTHDATA_ATTR_UNIONID, unionId);
    authData.put(AUTHDATA_ATTR_UNIONID_PLATFORM, unionIdPlatform);
    if (asMainAccount) {
      authData.put(AUTHDATA_ATTR_MAIN_ACCOUNT, asMainAccount);
    }
    return loginWithAuthData(clazz, authData, platform);
  }

  /**
   * login with auth data.
   * @param authData auth data.
   * @param platform platform string.
   * @param failOnNotExist flag to indicate to exit if failed or not.
   * @return observable instance.
   */
  public Observable<LCUser> loginWithAuthData(final Map<String, Object> authData, final String platform,
                                              final boolean failOnNotExist) {
    if (null == authData || authData.isEmpty()) {
      return Observable.error(new IllegalArgumentException(String.format(ILLEGALARGUMENT_MSG_FORMAT, "authData")));
    }
    if (StringUtil.isEmpty(platform)) {
      return Observable.error(new IllegalArgumentException(String.format(ILLEGALARGUMENT_MSG_FORMAT, "platform")));
    }

    HashMap<String, Object> data = createUserMapAFAP(getUsername(), null, getEmail(), getMobilePhoneNumber(), null);
    Map<String, Object> authMap = new HashMap<String, Object>();
    authMap.put(platform, authData);
    data.put(AUTHDATA_TAG, authMap);
    JSONObject param = JSONObject.Builder.create(data);
    return PaasClient.getStorageClient().signUpWithFlag(param, failOnNotExist).map(new Function<LCUser, LCUser>() {
      @Override
      public LCUser apply(LCUser avUser) throws Exception {
        LCUser.this.resetByRawData(avUser);
        LCUser.changeCurrentUser(LCUser.this, true);
        return LCUser.this;
      }
    });
  }

  /**
   * login with auth data.
   * @param authData auth data.
   * @param platform platform string.
   * @param unionId union id.
   * @param unionIdPlatform the platform which union id is binding with.
   * @param asMainAccount flag indicating that whether current platform is main account or not.
   * @param failOnNotExist flag to indicate to exit if failed or not.
   * @return observable instance.
   */
  public Observable<LCUser> loginWithAuthData(final Map<String, Object> authData, final String platform,
                                              final String unionId, final String unionIdPlatform,
                                              final boolean asMainAccount, final boolean failOnNotExist) {
    if (null == authData || authData.isEmpty()) {
      return Observable.error(new IllegalArgumentException(String.format(ILLEGALARGUMENT_MSG_FORMAT, "authData")));
    }
    if (StringUtil.isEmpty(unionId)) {
      return Observable.error(new IllegalArgumentException(String.format(ILLEGALARGUMENT_MSG_FORMAT, "unionId")));
    }
    if (StringUtil.isEmpty(unionIdPlatform)) {
      return Observable.error(new IllegalArgumentException(String.format(ILLEGALARGUMENT_MSG_FORMAT, "unionIdPlatform")));
    }
    authData.put(AUTHDATA_ATTR_UNIONID, unionId);
    authData.put(AUTHDATA_ATTR_UNIONID_PLATFORM, unionIdPlatform);
    if (asMainAccount) {
      authData.put(AUTHDATA_ATTR_MAIN_ACCOUNT, asMainAccount);
    }
    return loginWithAuthData(authData, platform, failOnNotExist);
  }

  /**
   * associate with third party data.
   * @param authData auth data.
   * @param platform platform name.
   * @return observable instance.
   */
  public Observable<LCUser> associateWithAuthData(Map<String, Object> authData, String platform) {
    if (null == authData || authData.isEmpty()) {
      return Observable.error(new IllegalArgumentException(String.format(ILLEGALARGUMENT_MSG_FORMAT, "authData")));
    }
    if (StringUtil.isEmpty(platform)) {
      return Observable.error(new IllegalArgumentException(String.format(ILLEGALARGUMENT_MSG_FORMAT, "platform")));
    }
    Map<String, Object> authDataAttr = new HashMap<String, Object>();
    authDataAttr.put(platform, authData);
    Object existedAuthData = this.get(AUTHDATA_TAG);
    if (existedAuthData instanceof Map) {
      authDataAttr.putAll((Map<String, Object>)existedAuthData);
    }
    this.put(AUTHDATA_TAG, authDataAttr);
    return (Observable<LCUser>) saveInBackground(new LCSaveOption().setFetchWhenSave(true));
  }

  /**
   * associate with third party data.
   * @param authData auth data.
   * @param platform platform name.
   * @param unionId union id.
   * @param unionIdPlatform the platform which union id is binding with.
   * @param asMainAccount flag indicating that whether current platform is main account or not.
   * @return observable instance.
   */
  public Observable<LCUser> associateWithAuthData(Map<String, Object> authData, String platform, String unionId, String unionIdPlatform,
                                                  boolean asMainAccount) {
    if (null == authData || authData.isEmpty()) {
      return Observable.error(new IllegalArgumentException(String.format(ILLEGALARGUMENT_MSG_FORMAT, "authData")));
    }
    if (StringUtil.isEmpty(unionId)) {
      return Observable.error(new IllegalArgumentException(String.format(ILLEGALARGUMENT_MSG_FORMAT, "unionId")));
    }
    if (StringUtil.isEmpty(unionIdPlatform)) {
      return Observable.error(new IllegalArgumentException(String.format(ILLEGALARGUMENT_MSG_FORMAT, "unionIdPlatform")));
    }
    authData.put(AUTHDATA_ATTR_UNIONID, unionId);
    authData.put(AUTHDATA_ATTR_UNIONID_PLATFORM, unionIdPlatform);
    if (asMainAccount) {
      authData.put(AUTHDATA_ATTR_MAIN_ACCOUNT, true);
    }
    return this.associateWithAuthData(authData, platform);
  }

  /**
   * dissociate with third party data.
   * @param platform platform name.
   * @return observable instance.
   */
  public Observable<LCUser> dissociateWithAuthData(final String platform) {
    if (StringUtil.isEmpty(platform)) {
      return Observable.error(new IllegalArgumentException(String.format(ILLEGALARGUMENT_MSG_FORMAT, "platform")));
    }

    String objectId = getObjectId();
    if (StringUtil.isEmpty(objectId) || !isAuthenticated()) {
      return Observable.error(new LCException(LCException.SESSION_MISSING,
              "the user object missing a valid session"));
    }
    this.remove(AUTHDATA_TAG + "." + platform);
    return this.saveInBackground().map(new Function<LCObject, LCUser>() {
      public LCUser apply(@NonNull LCObject var1) throws Exception {
        Map<String, Object> authData = (Map<String, Object>) LCUser.this.get(AUTHDATA_TAG);
        if (authData != null) {
          authData.remove(platform);
        }
        return LCUser.this;
      }
    });
  }

  /**
   * Session token operations
   * @return observable instance.
   */

  /**
   * check authenticated status in background.
   * @return observable instance.
   */
  public Observable<Boolean> checkAuthenticatedInBackground() {
    String sessionToken = getSessionToken();
    if (StringUtil.isEmpty(sessionToken)) {
      logger.d("sessionToken is not existed.");
      return Observable.just(false);
    }
    return PaasClient.getStorageClient().checkAuthenticated(sessionToken);
  }

  /**
   * refresh session token in background.
   * @return observable instance.
   */
  public Observable<Boolean> refreshSessionTokenInBackground() {
    return PaasClient.getStorageClient().refreshSessionToken(this);
  }

  /**
   * instantiate AVUser object with sessionToken(synchronized)
   * @param sessionToken session token
   * @return AVUser instance.
   *
   * this method DOES NOT change AVUser#currentUser, it makes sense for being called in lean engine.
   */
  public static LCUser becomeWithSessionToken(String sessionToken) {
    return becomeWithSessionToken(sessionToken, false);
  }

  /**
   * instantiate AVUser object with sessionToken(synchronized)
   * @param sessionToken session token
   * @param saveToCurrentUser flag indicating whether change current user or not.
   *                          true - save user to AVUser#currentUser.
   *                          false - not save.
   * @return AVUser instance.
   *
   */
  public static LCUser becomeWithSessionToken(String sessionToken, boolean saveToCurrentUser) {
    LCUser usr = becomeWithSessionTokenInBackground(sessionToken, saveToCurrentUser).blockingFirst();
    return usr;
  }

  /**
   * instantiate AVUser object with sessionToken(asynchronous)
   * @param sessionToken session token
   * @return Observable instance.
   *
   * this method DOES NOT change AVUser#currentUser, it makes sense for being called in lean engine.
   */
  public static Observable<? extends LCUser> becomeWithSessionTokenInBackground(String sessionToken) {
    return becomeWithSessionTokenInBackground(sessionToken, false);
  }

  /**
   * instantiate AVUser object with sessionToken(asynchronous)
   * @param sessionToken session token
   * @param saveToCurrentUser flag indicating whether change current user or not.
   *                          true - save user to AVUser#currentUser.
   *                          false - not save.
   * @return Observable instance.
   *
   */
  public static Observable<? extends LCUser> becomeWithSessionTokenInBackground(String sessionToken,
                                                                                boolean saveToCurrentUser) {
    return becomeWithSessionTokenInBackground(sessionToken, saveToCurrentUser, internalUserClazz());
  }

  /**
   * instantiate AVUser object with sessionToken(synchronized)
   * @param sessionToken session token
   * @param clazz class name.
   * @return AVUser instance.
   *
   * this method DOES NOT change AVUser#currentUser, it makes sense for being called in lean engine.
   */
  public static <T extends LCUser> T becomeWithSessionToken(String sessionToken, Class<T> clazz) {
    return becomeWithSessionToken(sessionToken, false, clazz);
  }

  /**
   * instantiate AVUser object with sessionToken(synchronized)
   * @param sessionToken session token
   * @param saveToCurrentUser flag indicating whether change current user or not.
   *                          true - save user to AVUser#currentUser.
   *                          false - not save.
   * @param clazz class name.
   * @param <T> template.
   * @return AVUser instance.
   *
   */
  public static <T extends LCUser> T becomeWithSessionToken(String sessionToken, boolean saveToCurrentUser,
                                                            Class<T> clazz) {
    T result = becomeWithSessionTokenInBackground(sessionToken, saveToCurrentUser, clazz).blockingFirst();
    return result;
  }

  /**
   * instantiate AVUser object with sessionToken(asynchronous)
   * @param sessionToken session token
   * @param clazz class name
   * @param <T> generic type.
   * @return Observable instance.
   *
   * this method DOES NOT change AVUser#currentUser, it makes sense for being called in lean engine.
   */
  public static <T extends LCUser> Observable<T> becomeWithSessionTokenInBackground(String sessionToken, Class<T> clazz) {
    return becomeWithSessionTokenInBackground(sessionToken, false, clazz);
  }

  /**
   * instantiate AVUser object with sessionToken(asynchronous)
   * @param sessionToken session token
   * @param saveToCurrentUser flag indicating whether change current user or not.
   *                          true - save user to AVUser#currentUser.
   *                          false - not save.
   * @param clazz class name
   * @param <T> generic type
   * @return Observable instance.
   */
  public static <T extends LCUser> Observable<T> becomeWithSessionTokenInBackground(String sessionToken,
                                                                                    final boolean saveToCurrentUser,
                                                                                    Class<T> clazz) {
    return PaasClient.getStorageClient().createUserBySession(sessionToken, clazz).map(new Function<T, T>() {
      @Override
      public T apply(T result) throws Exception {
        if (saveToCurrentUser) {
          LCUser.changeCurrentUser(result, true);
        }
        return result;
      }
    });
  }

  /**
   * user logout.
   */
  public static void logOut() {
    LCUser.changeCurrentUser(null, true);
  }

  /**
   * Get User Query
   * @param clazz class name.
   * @param <T> template type.
   * @return query instance.
   */
  public static <T extends LCUser> LCQuery<T> getUserQuery(Class<T> clazz) {
    return new LCQuery<T>(CLASS_NAME, clazz);
  }

  /**
   * Get User Query
   * @return query instance.
   */
  public static LCQuery<LCUser> getQuery() {
    return getQuery(LCUser.class);
  }

  /**
   * Get roles in background.
   * @return observable instance.
   */
  public Observable<List<LCRole>> getRolesInBackground() {
    LCQuery<LCRole> roleQuery = new LCQuery<LCRole>(LCRole.CLASS_NAME);
    roleQuery.whereEqualTo("users", this);
    return roleQuery.findInBackground();
  }

  /**
   * Current User Cache
   */
  transient private static boolean enableAutomatic = false;

  public static void enableAutomaticUser() {
    enableAutomatic = true;
  }

  public static boolean isEnableAutomatic() {
    return enableAutomatic;
  }

  public static void disableAutomaticUser() {
    enableAutomatic = false;
  }

  private static File currentUserArchivePath() {
    return new File(AppConfiguration.getDocumentDir() + "/currentUser");
  }

  private static boolean userArchiveExist() {
    return currentUserArchivePath().exists();
  }

  /**
   * change current user instance.
   * @param newUser new instance.
   * @param save flag indicating that whether save current user to cache or not.
   */
  public static synchronized void changeCurrentUser(LCUser newUser, boolean save) {
    if (AppConfiguration.isIncognitoMode()) {
      // disable current user in incognito mode.
      return;
    }
    if (null != newUser) {
      newUser.removeOperationForKey(ATTR_PASSWORD);
    }
    File currentUserArchivePath = currentUserArchivePath();
    if (null != newUser && save) {
      String jsonString = newUser.toJSONString();
      logger.d(jsonString);
      PersistenceUtil.sharedInstance().saveContentToFile(jsonString, currentUserArchivePath);
    } else if (save) {
      PersistenceUtil.sharedInstance().removeLock(currentUserArchivePath.getAbsolutePath());
      boolean deleteRst = currentUserArchivePath.delete();
      if (!deleteRst) {
        logger.w("failed to delete currentUser cache file.");
      }
    }
    PaasClient.getStorageClient().setCurrentUser(newUser);
  }

  /**
   * get current user, null if non-login.
   * @return user instance.
   */
  public static LCUser getCurrentUser() {
    return getCurrentUser(internalUserClazz());
  }

  /**
   * get current user, null if non-login.
   * @param userClass user object class.
   * @param <T> template type.
   * @return user instance.
   */
  public static <T extends LCUser> T getCurrentUser(Class<T> userClass) {
    if (AppConfiguration.isIncognitoMode()) {
      // disable current user in incognito mode.
      return null;
    }
    LCUser user = PaasClient.getStorageClient().getCurrentUser();
    if (null != user && userClass.isAssignableFrom(user.getClass())) {
      return (T) user;
    } else if (userArchiveExist()) {
      File currentUserArchivePath = currentUserArchivePath();
      synchronized (LCUser.class) {
        String jsonString = PersistenceUtil.sharedInstance().readContentFromFile(currentUserArchivePath);
        if (!StringUtil.isEmpty(jsonString)) {
          if (jsonString.indexOf("@type") >= 0 || jsonString.indexOf(ObjectDeserializer.KEY_VERSION) >= 0) {
            // new version.
            try {
              user = (LCUser) LCObject.parseLCObject(jsonString);;
              PaasClient.getStorageClient().setCurrentUser(user);
            } catch (Exception ex) {
              logger.w("failed to deserialize AVUser instance.", ex);
            }
          } else {
            // older format
            try {
              LCObject rawData = JSON.parseObject(jsonString, LCObject.class);
              T newUser = Transformer.transform(rawData, userClass);

              changeCurrentUser(newUser, true);
              user = newUser;
            } catch (Exception ex) {
              logger.w(ex);
            }
          }
        }
      }
    }
    if (enableAutomatic && null == user) {
      try {
        user = userClass.newInstance();
        changeCurrentUser(user, true);
      } catch (Exception ex) {
        logger.w(ex);
      }
    }
    return (T) Transformer.transform(user, userClass);
  }

  /**
   * Password-relative operations
   * @param email user email.
   * @return observable instance.
   */
  public static Observable<LCNull> requestPasswordResetInBackground(String email) {
    return PaasClient.getStorageClient().requestResetPassword(email);
  }

  /**
   * request sms code for resetting password
   * @param phoneNumber mobile phone number
   * @return observable instance
   */
  public static Observable<LCNull> requestPasswordResetBySmsCodeInBackground(String phoneNumber) {
    return requestPasswordResetBySmsCodeInBackground(phoneNumber, null);
  }

  /**
   * request sms code for resetting password, collaborating with AVCaptcha
   * @param phoneNumber mobile phone number
   * @param validateToken validated token, retrieved after invoking AVCaptcha#verifyCaptchaCodeInBackground
   * @return observable instance
   */
  public static Observable<LCNull> requestPasswordResetBySmsCodeInBackground(String phoneNumber, String validateToken) {
    return PaasClient.getStorageClient().requestResetPasswordBySmsCode(phoneNumber, validateToken);
  }

  /**
   * reset password with sms code for current user.
   * @param smsCode sms code
   * @param newPassword new password
   * @return observable instance
   */
  public static Observable<LCNull> resetPasswordBySmsCodeInBackground(String smsCode, String newPassword) {
    return PaasClient.getStorageClient().resetPasswordBySmsCode(smsCode, newPassword);
  }

  /**
   * update current user's password
   * @param oldPass old password
   * @param newPass new password
   * @return observable instance
   */
  public Observable<LCNull> updatePasswordInBackground(String oldPass, String newPass) {
    return PaasClient.getStorageClient().updatePassword(this, oldPass, newPass);
  }

  /**
   * request verified email
   * @param email email address
   * @return observable instance
   */
  public static Observable<LCNull> requestEmailVerifyInBackground(String email) {
    return PaasClient.getStorageClient().requestEmailVerify(email);
  }

  /**
   * request sms code for verification mobile phone.
   * @param mobilePhone mobile phone number.
   * @return observable instance
   */
  public static Observable<LCNull> requestMobilePhoneVerifyInBackground(String mobilePhone) {
    if (StringUtil.isEmpty(mobilePhone) || !LCSMS.checkMobilePhoneNumber(mobilePhone)) {
      return Observable.error(new IllegalArgumentException("mobile phone number is empty or invalid"));
    }
    return requestMobilePhoneVerifyInBackground(mobilePhone, null);
  }

  /**
   * request sms code for verification mobile phone, collaborating with AVCaptcha
   * @param mobilePhone mobile phone number.
   * @param validateToken validated token, retrieved after invoking AVCaptcha#verifyCaptchaCodeInBackground
   * @return observable instance.
   */
  public static Observable<LCNull> requestMobilePhoneVerifyInBackground(String mobilePhone, String validateToken) {
    if (StringUtil.isEmpty(mobilePhone) || !LCSMS.checkMobilePhoneNumber(mobilePhone)) {
      return Observable.error(new IllegalArgumentException("mobile phone number is empty or invalid"));
    }
    return PaasClient.getStorageClient().requestMobilePhoneVerify(mobilePhone, validateToken);
  }

  /**
   * request sms code for login
   * @param mobilePhone mobile phone number.
   * @return observable instance
   */
  public static Observable<LCNull> requestLoginSmsCodeInBackground(String mobilePhone) {
    if (StringUtil.isEmpty(mobilePhone) || !LCSMS.checkMobilePhoneNumber(mobilePhone)) {
      return Observable.error(new IllegalArgumentException("mobile phone number is empty or invalid"));
    }
    return requestLoginSmsCodeInBackground(mobilePhone, null);
  }

  /**
   * request sms code for login, collaborating with AVCaptcha
   * @param mobilePhone mobile phone number
   * @param validateToken validated token, retrieved after invoking AVCaptcha#verifyCaptchaCodeInBackground
   * @return observable instance.
   */
  public static Observable<LCNull> requestLoginSmsCodeInBackground(String mobilePhone, String validateToken) {
    if (StringUtil.isEmpty(mobilePhone) || !LCSMS.checkMobilePhoneNumber(mobilePhone)) {
      return Observable.error(new IllegalArgumentException("mobile phone number is empty or invalid"));
    }
    return PaasClient.getStorageClient().requestLoginSmsCode(mobilePhone, validateToken);
  }

  /**
   * verify sms code with current user's phone number.
   * @param verifyCode sms code
   * @return observable instance.
   */
  public static Observable<LCNull> verifyMobilePhoneInBackground(String verifyCode) {
    return PaasClient.getStorageClient().verifyMobilePhone(verifyCode);
  }

  /**
   * request sms code for updating phone number of current user.
   * @param mobilePhone mobile phone number.
   * @param option sms option
   * @return observable instance
   */
  public static Observable<LCNull> requestSMSCodeForUpdatingPhoneNumberInBackground(String mobilePhone, LCSMSOption option) {
    return requestSMSCodeForUpdatingPhoneNumberInBackground(null, mobilePhone, option);
  }

  /**
   * request sms code for updating phone number of current user.
   * @param asAuthenticatedUser explicit user for request authentication.
   * @param mobilePhone mobile phone number.
   * @param option sms option
   * @return observable instance
   *
   * in general, this method should be invoked in lean engine.
   */
  public static Observable<LCNull> requestSMSCodeForUpdatingPhoneNumberInBackground(LCUser asAuthenticatedUser,
                                                                                    String mobilePhone, LCSMSOption option) {
    if (StringUtil.isEmpty(mobilePhone) || !LCSMS.checkMobilePhoneNumber(mobilePhone)) {
      return Observable.error(new IllegalArgumentException("mobile phone number is empty or invalid"));
    }
    Map<String, Object> param = (null == option)? new HashMap<String, Object>() : option.getOptionMap();
    return PaasClient.getStorageClient().requestSMSCodeForUpdatingPhoneNumber(asAuthenticatedUser, mobilePhone, param);
  }

  /**
   * verify sms code for updating phone number of current user.
   * @param code    sms code
   * @param mobilePhone mobile phone number.
   * @return observable instance
   */
  public static Observable<LCNull> verifySMSCodeForUpdatingPhoneNumberInBackground(String code, String mobilePhone) {
    return verifySMSCodeForUpdatingPhoneNumberInBackground(null, code, mobilePhone);
  }

  /**
   * verify sms code for updating phone number of current user.
   * @param asAuthenticatedUser explicit user for request authentication.
   * @param code    sms code
   * @param mobilePhone mobile phone number.
   * @return observable instance
   *
   * in general, this method should be invoked in lean engine.
   */
  public static Observable<LCNull> verifySMSCodeForUpdatingPhoneNumberInBackground(LCUser asAuthenticatedUser,
                                                                                   String code, String mobilePhone) {
    if (StringUtil.isEmpty(code) || StringUtil.isEmpty(mobilePhone)) {
      return Observable.error(new IllegalArgumentException("code or mobilePhone is empty"));
    }
    return PaasClient.getStorageClient().verifySMSCodeForUpdatingPhoneNumber(asAuthenticatedUser, code, mobilePhone);
  }

  private boolean checkUserAuthentication(final LCCallback callback) {
    if (!this.isAuthenticated() || StringUtil.isEmpty(getObjectId())) {
      if (callback != null) {
        callback.internalDone(ErrorUtils.propagateException(LCException.SESSION_MISSING,
                "No valid session token, make sure signUp or login has been called."));
      }
      return false;
    }
    return true;
  }

  /**
   * follow somebody in background.
   * @param userObjectId  target user objectId.
   * @return observable instance.
   */
  public Observable<JSONObject> followInBackground(String userObjectId) {
    return followInBackground(null, userObjectId);
  }

  /**
   * follow somebody in background.
   * @param asAuthenticatedUser explicit user for request authentication.
   * @param userObjectId target user objectId.
   * @return observable instance.
   *
   * in general, this method should be invoked in lean engine.
   */
  public Observable<JSONObject> followInBackground(LCUser asAuthenticatedUser, String userObjectId) {
    return this.followInBackground(asAuthenticatedUser, userObjectId, new HashMap<String, Object>());
  }

  /**
   * follow somebody in background.
   * @param userObjectId target user objectId.
   * @param attributes friendship attributes.
   * @return observable instance.
   */
  public Observable<JSONObject> followInBackground(String userObjectId, Map<String, Object> attributes) {
    return followInBackground(null, userObjectId, attributes);
  }

  /**
   * follow somebody in background.
   * @param asAuthenticatedUser explicit user for request authentication.
   * @param userObjectId target user objectId.
   * @param attributes friendship attributes.
   * @return observable instance.
   *
   * in general, this method should be invoked in lean engine.
   */
  public Observable<JSONObject> followInBackground(LCUser asAuthenticatedUser, String userObjectId, Map<String, Object> attributes) {
    if (!checkUserAuthentication(null)) {
      return Observable.error(ErrorUtils.propagateException(LCException.SESSION_MISSING,
              "No valid session token, make sure signUp or login has been called."));
    }
    return PaasClient.getStorageClient().followUser(asAuthenticatedUser, getObjectId(), userObjectId, attributes);
  }

  /**
   * unfollow somebody in background.
   * @param userObjectId target user objectId.
   * @return observable instance.
   */
  public Observable<JSONObject> unfollowInBackground(String userObjectId) {
    return unfollowInBackground(null, userObjectId);
  }

  /**
   * unfollow somebody in background.
   * @param asAuthenticatedUser explicit user for request authentication.
   * @param userObjectId target user objectId.
   * @return observable instance.
   *
   * in general, this method should be invoked in lean engine.
   */
  public Observable<JSONObject> unfollowInBackground(LCUser asAuthenticatedUser, String userObjectId) {
    if (!checkUserAuthentication(null)) {
      return Observable.error(ErrorUtils.propagateException(LCException.SESSION_MISSING,
              "No valid session token, make sure signUp or login has been called."));
    }
    return PaasClient.getStorageClient().unfollowUser(asAuthenticatedUser, getObjectId(), userObjectId);
  }

  /**
   * get follower query.
   * @return query instance.
   */
  public LCQuery<LCObject> followerQuery() {
    return LCUser.followerQuery(getObjectId(), LCObject.class);
  }

  /**
   * get followee query.
   * @return query instance.
   */
  public LCQuery<LCObject> followeeQuery() {
    return LCUser.followeeQuery(getObjectId(), LCObject.class);
  }

  /**
   * get follower query.
   * @param userObjectId user object id.
   * @param clazz result class.
   * @param <T> template type.
   * @return query instance.
   */
  public static <T extends LCObject> LCQuery<T> followerQuery(final String userObjectId,
                                                              Class<T> clazz) {
    if (StringUtil.isEmpty(userObjectId)) {
      throw new IllegalArgumentException("Blank user objectId");
    }
    LCQuery<T> query = new LCQuery<>("_Follower", clazz);
    query.whereEqualTo("user", LCUser.createWithoutData(CLASS_NAME, userObjectId));
    query.include("follower");
    return query;
  }

  /**
   * get followee query.
   * @param userObjectId user object id.
   * @param clazz result class.
   * @param <T> template type.
   * @return query instance.
   */
  public static <T extends LCObject> LCQuery<T> followeeQuery(final String userObjectId, Class<T> clazz) {
    if (StringUtil.isEmpty(userObjectId)) {
      throw new IllegalArgumentException("Blank user objectId");
    }
    LCQuery<T> query = new LCQuery<>("_Followee", clazz);
    query.whereEqualTo("user", LCUser.createWithoutData(CLASS_NAME, userObjectId));
    query.include("followee");
    return query;
  }

  /**
   * get friendship query of current user.
   *
   * @param isFollowerDirection query direction:
   *                            true - query follower of current user(users which followed current user).
   *                            false - query followee of current user(users which current user followed).
   * @return query instance, null for non-authenticated user.
   */
  public LCQuery<LCFriendship> friendshipQuery(boolean isFollowerDirection) {
    String userObjectId = getObjectId();
    if (StringUtil.isEmpty(userObjectId)) {
      logger.d("user object id is empty.");
      return null;
    }
    LCQuery<LCFriendship> query = new LCQuery<>(LCFriendship.CLASS_NAME);
    if (isFollowerDirection) {
      query.whereEqualTo(LCFriendship.ATTR_FOLLOWEE, LCUser.createWithoutData(CLASS_NAME, userObjectId));
      query.include(LCFriendship.ATTR_USER);
    } else {
      query.whereEqualTo(LCFriendship.ATTR_USER, LCUser.createWithoutData(CLASS_NAME, userObjectId));
      query.include(LCFriendship.ATTR_FOLLOWEE);
    }
    query.whereEqualTo(LCFriendship.ATTR_FRIEND_STATUS, true);
    return query;
  }

  /**
   * query current user's friendship.
   * @return Observable instance to monitor operation result.
   */
  public Observable<List<LCFriendship>> queryFriendship() {
    return this.queryFriendship(0, 0, null);
  }

  /**
   * query current user's friendship.
   * @param offset result offset.
   * @param limit result size limit.
   * @return Observable instance to monitor operation result.
   */
  public Observable<List<LCFriendship>> queryFriendship(int offset, int limit, String orderBy) {
    QueryConditions conditions = new QueryConditions();
    conditions.whereEqualTo(LCFriendship.ATTR_FRIEND_STATUS, true);
    if (offset > 0) {
      conditions.setSkip(offset);
    }
    if (limit > 0) {
      conditions.setLimit(limit);
    }
    if (!StringUtil.isEmpty(orderBy)) {
      conditions.setOrder(orderBy);
    }
    return PaasClient.getStorageClient().queryFriendship(this, conditions.assembleParameters());
  }

  /**
   * apply new friendship to someone.
   *
   * @param friend target user.
   * @param attributes additional attributes.
   * @return Observable instance to monitor operation result.
   */
  public Observable<LCFriendshipRequest> applyFriendshipInBackground(LCUser friend, Map<String, Object> attributes) {
    return applyFriendshipInBackground(null, friend, attributes);
  }

  /**
   * apply new friendship to someone.
   * @param asAuthenticatedUser explicit user for request authentication.
   * @param friend target user.
   * @param attributes additional attributes.
   * @return Observable instance to monitor operation result.
   *
   * in general, this method should be invoked in lean engine.
   */
  public Observable<LCFriendshipRequest> applyFriendshipInBackground(LCUser asAuthenticatedUser,
                                                                     LCUser friend, Map<String, Object> attributes) {
    if (!checkUserAuthentication(null)) {
      logger.d("current user isn't authenticated.");
      return Observable.error(ErrorUtils.propagateException(LCException.SESSION_MISSING,
              "No valid session token, make sure signUp or login has been called."));
    }
    if (null == friend || StringUtil.isEmpty(friend.getObjectId())) {
      return Observable.error(ErrorUtils.propagateException(LCException.INVALID_PARAMETER,
              "friend user is invalid."));
    }
    Map<String, Object> param = new HashMap<>();
    param.put(LCFriendshipRequest.ATTR_USER, Utils.getParsedObject(this));
    param.put(LCFriendshipRequest.ATTR_FRIEND, Utils.getParsedObject(friend));
    if (null != attributes && attributes.size() > 0) {
      param.put(PARAM_ATTR_FRIENDSHIP, attributes);
    }
    JSONObject jsonObject = JSONObject.Builder.create(param);
    return PaasClient.getStorageClient().applyFriendshipRequest(asAuthenticatedUser, jsonObject);
  }

  /**
   * update friendship attributes.
   *
   * @param friendship friendship instance.
   * @return observable instance.
   */
  public Observable<LCFriendship> updateFriendship(LCFriendship friendship) {
    return updateFriendship(null, friendship);
  }

  /**
   * update friendship attributes.
   * @param asAuthenticatedUser explicit user for request authentication.
   * @param friendship friendship instance.
   * @return observable instance.
   *
   * in general, this method should be invoked in lean engine.
   */
  public Observable<LCFriendship> updateFriendship(LCUser asAuthenticatedUser, LCFriendship friendship) {
    if (!checkUserAuthentication(null)) {
      logger.d("current user isn't authenticated.");
      return Observable.error(ErrorUtils.propagateException(LCException.SESSION_MISSING,
              "No valid session token, make sure signUp or login has been called."));
    }
    if (null == friendship || StringUtil.isEmpty(friendship.getObjectId())) {
      return Observable.error(ErrorUtils.propagateException(LCException.INVALID_PARAMETER,
              "friendship request(objectId) is invalid."));
    }
    if (null == friendship.getFollowee() || StringUtil.isEmpty(friendship.getFollowee().getObjectId())) {
      return Observable.error(ErrorUtils.propagateException(LCException.INVALID_PARAMETER,
              "friendship request(followee) is invalid."));
    }
    JSONObject changedParam = friendship.generateChangedParam();
    if (null == changedParam || changedParam.size() < 1) {
      logger.d("nothing is changed within friendship.");
      return Observable.just(friendship);
    }
    HashMap<String, Object> param = new HashMap<>();
    param.put(PARAM_ATTR_FRIENDSHIP, changedParam);
    return PaasClient.getStorageClient().updateFriendship(asAuthenticatedUser,
            getObjectId(), friendship.getFollowee().getObjectId(), param);
  }

  /**
   * accept a friendship.
   * @param request friendship request.
   * @param attributes additional attributes.
   * @return Observable instance to monitor operation result.
   * notice: attributes is necessary as parameter bcz they are not properties of FriendshipRequest.
   */
  public Observable<LCFriendshipRequest> acceptFriendshipRequest(LCFriendshipRequest request,
                                                                 Map<String, Object> attributes) {
    return acceptFriendshipRequest(null, request, attributes);
  }

  /**
   * accept a friendship.
   * @param asAuthenticatedUser explicit user for request authentication.
   * @param request friendship request.
   * @param attributes additional attributes.
   * @return Observable instance to monitor operation result.
   * notice: attributes is necessary as parameter bcz they are not properties of FriendshipRequest.
   *
   * in general, this method should be invoked in lean engine.
   */
  public Observable<LCFriendshipRequest> acceptFriendshipRequest(LCUser asAuthenticatedUser,
                                                                 LCFriendshipRequest request,
                                                                 Map<String, Object> attributes){
    if (!checkUserAuthentication(null)) {
      logger.d("current user isn't authenticated.");
      return Observable.error(ErrorUtils.propagateException(LCException.SESSION_MISSING,
              "No valid session token, make sure signUp or login has been called."));
    }
    if (null == request || StringUtil.isEmpty(request.getObjectId())) {
      return Observable.error(ErrorUtils.propagateException(LCException.INVALID_PARAMETER,
              "friendship request(objectId) is invalid."));
    }

    HashMap<String, Object> param = new HashMap<>();
    if (null != attributes && attributes.size() > 0) {
      param.put(PARAM_ATTR_FRIENDSHIP, attributes);
    }
    JSONObject jsonObject = JSONObject.Builder.create(param);
    return PaasClient.getStorageClient().acceptFriendshipRequest(asAuthenticatedUser, request, jsonObject);
  }

  /**
   * decline a friendship.
   * @param request friendship request.
   * @return Observable instance to monitor operation result.
   */
  public Observable<LCFriendshipRequest> declineFriendshipRequest(LCFriendshipRequest request) {
    return declineFriendshipRequest(null, request);
  }

  /**
   * decline a friendship.
   * @param asAuthenticatedUser explicit user for request authentication.
   * @param request friendship request.
   * @return Observable instance to monitor operation result.
   *
   * in general, this method should be invoked in lean engine.
   */
  public Observable<LCFriendshipRequest> declineFriendshipRequest(LCUser asAuthenticatedUser, LCFriendshipRequest request) {
    if (!checkUserAuthentication(null)) {
      logger.d("current user isn't authenticated.");
      return Observable.error(ErrorUtils.propagateException(LCException.SESSION_MISSING,
              "No valid session token, make sure signUp or login has been called."));
    }
    if (null == request || StringUtil.isEmpty(request.getObjectId())) {
      return Observable.error(ErrorUtils.propagateException(LCException.INVALID_PARAMETER,
              "friendship request(objectId) is invalid."));
    }

    return PaasClient.getStorageClient().declineFriendshipRequest(asAuthenticatedUser, request);
  }

  /**
   * get query for AVFriendshipRequest.
   *
   * @param status request status. following value can be used individually or combined with `and` operator:
   *               AVFriendshipRequest.STATUS_PENDING(0x01) - request is pending yet.
   *               AVFriendshipRequest.STATUS_ACCEPTED(0x02) - request is accepted by user.
   *               AVFriendshipRequest.STATUS_DECLINED(0x04) - request is declined by user.
   *               AVFriendshipRequest.STATUS_ANY(0x07) - no matter status, all of requests are wanted by current query.
   * @param includeTargetUser boolean flag, indicating that need to include target user pointer or not.
   * @param requestToMe boolean flag, indicating all requests are sent to current user or not.
   *                    true - someone others sent requests to current user.
   *                    false - current user sent requests to others.
   * @return AVFriendshipRequest query, null for current user isn't authenticated or status is invlaid.
   */
  public LCQuery<LCFriendshipRequest> friendshipRequestQuery(int status,
                                                             boolean includeTargetUser, boolean requestToMe) {
    if (!checkUserAuthentication(null)) {
      logger.d("current user isn't authenticated.");
      return null;
    }
    List<String> statusCondition = new ArrayList<>(1);
    if ((status & LCFriendshipRequest.STATUS_PENDING) == LCFriendshipRequest.STATUS_PENDING) {
      statusCondition.add(LCFriendshipRequest.RequestStatus.Pending.name().toLowerCase());
    }
    if ((status & LCFriendshipRequest.STATUS_ACCEPTED) == LCFriendshipRequest.STATUS_ACCEPTED) {
      statusCondition.add(LCFriendshipRequest.RequestStatus.Accepted.name().toLowerCase());
    }
    if ((status & LCFriendshipRequest.STATUS_DECLINED) == LCFriendshipRequest.STATUS_DECLINED) {
      statusCondition.add(LCFriendshipRequest.RequestStatus.Declined.name().toLowerCase());
    }
    if (statusCondition.size() < 1) {
      logger.d("status parameter is invalid.");
      return null;
    }

    LCQuery<LCFriendshipRequest> result = new LCQuery<>(LCFriendshipRequest.CLASS_NAME);
    result.whereContainedIn(LCFriendshipRequest.ATTR_STATUS, statusCondition);
    if (requestToMe) {
      result.whereEqualTo(LCFriendshipRequest.ATTR_FRIEND, this);
      if (includeTargetUser) {
        result.include(LCFriendshipRequest.ATTR_USER);
      }
    } else {
      result.whereEqualTo(LCFriendshipRequest.ATTR_USER, this);
      if (includeTargetUser) {
        result.include(LCFriendshipRequest.ATTR_FRIEND);
      }
    }
    result.addDescendingOrder(LCObject.KEY_UPDATED_AT);
    return result;
  }

  private void processResultList(List<JSONObject> results, List<LCUser> list, String tag) {
    for (JSONObject item : results) {
      if (null != item) {
        LCUser user = (LCUser) Utils.parseObjectFromMap((Map<String, Object>)item.get(tag));
        list.add(user);
      }
    }
  }
  private Map<String, List<LCUser>> parseFollowerAndFollowee(JSONObject jsonObject) {
    Map<String, List<LCUser>> map = new HashMap<String, List<LCUser>>();
    if (null != jsonObject) {
      List<JSONObject> followers = LCUtils.getObjectListFromMapList((List<Map<String, Object>>)jsonObject.get("followers"));
      if ( null != followers && followers.size() > 0) {
        List<LCUser> followerUsers = new LinkedList<LCUser>();
        processResultList(followers, followerUsers, FOLLOWER_TAG);
        map.put(FOLLOWER_TAG, followerUsers);
      }
      List<JSONObject> followees = LCUtils.getObjectListFromMapList((List<Map<String, Object>>)jsonObject.get("followees"));
      if (null != followees && followees.size() > 0) {
        List<LCUser> followeeUsers = new LinkedList<LCUser>();
        processResultList(followees, followeeUsers, FOLLOWEE_TAG);
        map.put(FOLLOWEE_TAG, followeeUsers);
      }
    }
    return map;
  }

  /**
   * get follower and followee in background.
   * @param callback callback handler.
   *
   * request authentication with current user.
   */
  public void getFollowersAndFolloweesInBackground(final FollowersAndFolloweesCallback callback) {
    getFollowersAndFolloweesInBackground(null, callback);
  }

  /**
   * get follower and followee in background.
   * @param asAuthenticatedUser explicit user for request authentication.
   * @param callback callback handler.
   *
   * in general, this method should be invoked in lean engine.
   */
  public void getFollowersAndFolloweesInBackground(LCUser asAuthenticatedUser,
                                                   final FollowersAndFolloweesCallback callback) {
    if (null == callback) {
      return;
    }
    if (!checkUserAuthentication(callback)) {
      return;
    }
    PaasClient.getStorageClient().getFollowersAndFollowees(asAuthenticatedUser, getObjectId())
            .subscribe(new Observer<JSONObject>() {
      @Override
      public void onSubscribe(Disposable disposable) {

      }

      @Override
      public void onNext(JSONObject jsonObject) {
        if (null == jsonObject) {
          callback.done(null, null);
        } else {
          Map<String, List<LCUser>> result = parseFollowerAndFollowee(jsonObject);
          callback.done(result, null);
        }
      }

      @Override
      public void onError(Throwable throwable) {
        callback.done(null, new LCException(throwable));
      }

      @Override
      public void onComplete() {
      }
    });
  }

  /**
   * 通过设置此方法，所有关联对象中的 AVUser 对象都会被强转成注册的 AVUser 子类对象
   * @param clazz class name
   */
  public static void alwaysUseSubUserClass(Class<? extends LCUser> clazz) {
    LCUser.registerSubclass(clazz);
    subClazz = clazz;
  }
  private static Class internalUserClazz() {
    return subClazz == null ? LCUser.class: subClazz;
  }

  /**
   * 通过这个方法可以将 AVUser 对象强转为其子类对象
   *
   * @param user  user object.
   * @param clazz user class name.
   * @param <T> template type.
   * @return user subclass instance.
   */
  public static <T extends LCUser> T cast(LCUser user, Class<T> clazz) {
    try {
      T newUser = LCObject.cast(user, clazz);
      return newUser;
    } catch (Exception e) {

    }
    return null;
  }
}
