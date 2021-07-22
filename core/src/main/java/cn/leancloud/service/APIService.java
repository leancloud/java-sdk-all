package cn.leancloud.service;

import cn.leancloud.*;
import cn.leancloud.json.JSONObject;
import cn.leancloud.query.LCQueryResult;
import cn.leancloud.search.LCSearchResponse;
import cn.leancloud.sms.LCCaptchaDigest;
import cn.leancloud.sms.LCCaptchaValidateResult;
import cn.leancloud.types.LCDate;
import cn.leancloud.types.LCNull;
import cn.leancloud.upload.FileUploadToken;
import io.reactivex.Observable;
import retrofit2.Call;
import retrofit2.http.*;

import java.util.List;
import java.util.Map;

public interface APIService {
  String HEADER_KEY_LC_SESSIONTOKEN = "X-LC-Session";

  /**
   * Object Operations.
   */

  @GET("/1.1/classes/{className}")
  Observable<List<? extends LCObject>> findObjects(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                                   @Path("className") String className);

  @GET("/1.1/classes/{className}")
  Observable<LCQueryResult> queryObjects(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                         @Path("className") String className, @QueryMap Map<String, String> query);

  @GET("/1.1/cloudQuery")
  Observable<LCQueryResult> cloudQuery(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                       @QueryMap Map<String, String> query);

  @GET("/1.1/classes/{className}/{objectId}")
  Observable<LCObject> fetchObject(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                   @Path("className") String className, @Path("objectId") String objectId);

  @GET("/1.1/classes/{className}/{objectId}")
  Observable<LCObject> fetchObject(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                   @Path("className") String className, @Path("objectId") String objectId,
                                   @Query("include") String includeKeys);

  @POST("/1.1/classes/{className}")
  Observable<LCObject> createObject(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                    @Path("className") String className, @Body JSONObject object,
                                    @Query("fetchWhenSave") boolean fetchFlag,
                                    @Query("where") JSONObject where);

  @PUT("/1.1/classes/{className}/{objectId}")
  Observable<LCObject> updateObject(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                    @Path("className") String className, @Path("objectId") String objectId,
                                    @Body JSONObject object, @Query("fetchWhenSave") boolean fetchFlag,
                                    @Query("where") JSONObject where);

  @HTTP(method = "DELETE", path = "/1.1/classes/{className}/{objectId}", hasBody = true)
  Observable<LCNull> deleteObject(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                  @Path("className") String className, @Path("objectId") String objectId,
                                  @Body Map<String, Object> param);

  @POST("/1.1/batch")
  Observable<List<Map<String, Object>>> batchCreate(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                                    @Body JSONObject param);

  /**
   * AVInstalltion methods.
   */

  @POST("/1.1/{endpointClass}")
  Observable<LCObject> saveWholeObject(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                       @Path("endpointClass") String endpointClass, @Body JSONObject object,
                                       @Query("fetchWhenSave") boolean fetchFlag,
                                       @Query("where") JSONObject where);
  @PUT("/1.1/{endpointClass}/{objectId}")
  Observable<LCObject> saveWholeObject(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                       @Path("endpointClass") String endpointClass, @Path("objectId") String objectId,
                                       @Body JSONObject object, @Query("fetchWhenSave") boolean fetchFlag,
                                       @Query("where") JSONObject where);
  @GET("/1.1/{endpointClass}/{objectId}")
  Observable<LCObject> getWholeObject(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                      @Path("endpointClass") String endpointClass, @Path("objectId") String objectId,
                                      @Query("include") String includeKeys);
  @HTTP(method = "DELETE", path = "/1.1/{endpointClass}/{objectId}", hasBody = true)
  Observable<LCNull> deleteWholeObject(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                       @Path("endpointClass") String endpointClass, @Path("objectId") String objectId,
                                       @Body Map<String, Object> param);

  /**
   * request format:
   *    requests: [unit, unit]
   * unit format:
   *    {"path":"/1.1/classes/{class}/{objectId}",
   *     "method":"PUT",
   *     "body":{"{field}":operationJson,
   *             "__internalId":"{objectId}",
   *             "__children":[]},
   *     "params":{}
   *    }
   * for update same field with multiple operations, we must use batchUpdate instead of batchSave,
   * otherwise, `__internalId` will become a common field of target instance.
   */
  @POST("/1.1/batch/save")
  Observable<JSONObject> batchUpdate(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                     @Body JSONObject param);

  /**
   * Cloud Functions
   */
  @POST("/1.1/functions/{name}")
  Observable<Map<String, Object>> cloudFunction(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                                @Path("name") String functionName, @Body Map<String, Object> param);

  @POST("/1.1/call/{name}")
  Observable<Map<String, Object>> cloudRPC(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                           @Path("name") String functionName, @Body Object param);

  /**
   * File Operations.
   */

  @POST("/1.1/fileTokens")
  Observable<FileUploadToken> createUploadToken(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                                @Body JSONObject fileData);

  @POST("/1.1/fileCallback")
  Call<LCNull> fileCallback(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                            @Body JSONObject result);

  @GET("/1.1/files/{objectId}")
  Observable<LCFile> fetchFile(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                               @Path("objectId") String objectId);

  @GET("/1.1/date")
  Observable<LCDate> currentTimeMillis();

  /**
   * Role Operations.
   */
  @POST("/1.1/roles")
  Observable<LCRole> createRole(@Body JSONObject object);

  /**
   * User Operations.
   */

  @POST("/1.1/users")
  Observable<LCUser> signup(@Body JSONObject object);
  @POST("/1.1/users")
  Observable<LCUser> signup(@Body JSONObject object, @Query("failOnNotExist") boolean failOnNotExist);
  @GET("/1.1/users")
  Observable<LCQueryResult> queryUsers(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                       @QueryMap Map<String, String> query);

  @POST("/1.1/users/friendshipRequests")
  Observable<LCObject> applyFriendship(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                       @Body JSONObject param);

  @PUT("/1.1/users/friendshipRequests/{requestId}/accept")
  Observable<LCObject> acceptFriendshipRequest(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                               @Path("requestId") String requestId,
                                               @Body JSONObject param);

  @PUT("/1.1/users/friendshipRequests/{requestId}/decline")
  Observable<LCObject> declineFriendshipRequest(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                                @Path("requestId") String requestId);

  @POST("/1.1/usersByMobilePhone")
  Observable<LCUser> signupByMobilePhone(@Body JSONObject object);

  @POST("/1.1/login")
  Observable<LCUser> login(@Body JSONObject object);

  @PUT("/1.1/users/{objectId}/updatePassword")
  Observable<LCUser> updatePassword(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                    @Path("objectId") String objectId, @Body JSONObject object);

  @GET("/1.1/users/me")
  Observable<LCUser> checkAuthenticated(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                        @QueryMap Map<String, String> query);

  @PUT("/1.1/users/{objectId}/refreshSessionToken")
  Observable<LCUser> refreshSessionToken(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                         @Path("objectId") String objectId);

  @POST("/1.1/requestPasswordReset")
  Observable<LCNull> requestResetPassword(@Body Map<String, String> param);

  @POST("/1.1/requestPasswordResetBySmsCode")
  Observable<LCNull> requestResetPasswordBySmsCode(@Body Map<String, String> param);

  @PUT("/1.1/resetPasswordBySmsCode/{smsCode}")
  Observable<LCNull> resetPasswordBySmsCode(@Path("smsCode") String smsCode, @Body Map<String, String> param);

  @POST("/1.1/requestEmailVerify")
  Observable<LCNull> requestEmailVerify(@Body Map<String, String> param);

  @POST("/1.1/requestMobilePhoneVerify")
  Observable<LCNull> requestMobilePhoneVerify(@Body Map<String, String> param);

  @POST("/1.1/verifyMobilePhone/{verifyCode}")
  Observable<LCNull> verifyMobilePhone(@Path("verifyCode") String verifyCode);

  @POST("/1.1/requestLoginSmsCode")
  Observable<LCNull> requestLoginSmsCode(@Body Map<String, String> param);

  @POST("/1.1/users/{followee}/friendship/{follower}")
  Observable<JSONObject> followUser(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                    @Path("followee") String followee, @Path("follower") String follower,
                                    @Body Map<String, Object> param);

  @PUT("/1.1/users/{followee}/friendship/{friendId}")
  Observable<LCFriendship> updateFriendship(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                            @Path("followee") String followee, @Path("friendId") String friendId,
                                            @Body Map<String, Object> param);

  @DELETE("/1.1/users/{followee}/friendship/{follower}")
  Observable<JSONObject> unfollowUser(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                      @Path("followee") String followee, @Path("follower") String follower);

  @GET("/1.1/users/{userId}/followers")
  Observable<LCQueryResult> getFollowers(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                        @Path("userId") String userId,
                                        @QueryMap Map<String, String> query);

  @GET("/1.1/users/{userId}/followees")
  Observable<LCQueryResult> getFollowees(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                        @Path("userId") String userId,
                                        @QueryMap Map<String, String> query);

  @GET("/1.1/users/{userId}/followersAndFollowees")
  Observable<JSONObject> getFollowersAndFollowees(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                                  @Path("userId") String userId);

  /**
   * Status API
   */
  @POST("/1.1/statuses")
  Observable<LCStatus> postStatus(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                  @Body Map<String, Object> param);

  @GET("/1.1/statuses/{statusId}")
  Observable<LCStatus> fetchSingleStatus(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                         @Path("statusId") String statusId);

  @GET("/1.1/statuses")
  Observable<LCQueryResult> fetchStatuses(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                          @QueryMap Map<String, String> query);

  @DELETE("/1.1/statuses/{statusId}")
  Observable<LCNull> deleteStatus(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                  @Path("statusId") String statusId);

  @DELETE("/1.1/subscribe/statuses/inbox")
  Observable<LCNull> deleteInboxStatus(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                       @QueryMap Map<String, Object> query);

  @GET("/1.1/subscribe/statuses")
  Observable<LCQueryResult> queryInbox(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                       @QueryMap Map<String, String> query);

  @GET("/1.1/subscribe/statuses/count")
  Observable<JSONObject> getInboxCount(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                       @QueryMap Map<String, String> query);

  @POST("/1.1/subscribe/statuses/resetUnreadCount")
  Observable<LCNull> resetInboxUnreadCount(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken);


  /**
   * SMS / Capture requests
   */
  @GET("/1.1/requestCaptcha")
  Observable<LCCaptchaDigest> requestCaptcha(@QueryMap Map<String, String> query);

  @POST("/1.1/verifyCaptcha")
  Observable<LCCaptchaValidateResult> verifyCaptcha(@Body Map<String, String> param);

  @POST("/1.1/requestSmsCode")
  Observable<LCNull> requestSMSCode(@Body Map<String, Object> param);

  @POST("/1.1/verifySmsCode/{code}")
  Observable<LCNull> verifySMSCode(@Path("code") String code, @Body Map<String, Object> param);

  @POST("/1.1/requestChangePhoneNumber")
  Observable<LCNull> requestSMSCodeForUpdatingPhoneNumber(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                                          @Body Map<String, Object> param);

  @POST("/1.1/changePhoneNumber")
  Observable<LCNull> verifySMSCodeForUpdatingPhoneNumber(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                                         @Body Map<String, Object> param);

  /**
   * FullText Search API
   */
  @GET("/1.1/search/select")
  Observable<LCSearchResponse> search(@Header(HEADER_KEY_LC_SESSIONTOKEN) String sessionToken,
                                      @QueryMap Map<String, String> query);
}
