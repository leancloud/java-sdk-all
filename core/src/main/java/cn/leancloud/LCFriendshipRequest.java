package cn.leancloud;

import cn.leancloud.annotation.LCClassName;
import cn.leancloud.utils.ErrorUtils;
import io.reactivex.Observable;

import java.util.Map;

@LCClassName("_FriendshipRequest")
public class LCFriendshipRequest extends LCObject {
  public static final String CLASS_NAME = "_FriendshipRequest";
  public static final String ATTR_FRIEND = "friend";
  public static final String ATTR_USER = "user";
  public static final String ATTR_STATUS = "status";

  public static final String INTERNAL_STATUS_ACCEPTED = "accepted";
  public static final String INTERNAL_STATUS_DECLINED = "declined";
  public static final int STATUS_PENDING = 0x01;
  public static final int STATUS_ACCEPTED = 0x02;
  public static final int STATUS_DECLINED = 0x04;
  public static final int STATUS_ANY = STATUS_PENDING | STATUS_ACCEPTED | STATUS_DECLINED;

  enum RequestStatus {
    Pending, Accepted, Declined
  }

  public LCFriendshipRequest() {
    super(CLASS_NAME);
  }

  public LCFriendshipRequest(LCFriendshipRequest other) {
    super(other);
  }

  public void setFriend(LCUser user) {
    put(ATTR_FRIEND, user);
  }

  public LCUser getFriend() {
    return getLCObject(ATTR_FRIEND);
  }

  public LCUser getSourceUser() {
    return getLCObject(ATTR_USER);
  }

  public void setSourceUser(LCUser user) {
    put(ATTR_USER, user);
  }

  /**
   * accept friend request by current user.
   *
   * @param attributes additional attributes for this friend.
   * @return observable instance
   *
   * notice: attributes is necessary as parameter bcz they are not properties of FriendshipRequest.
   */
  public Observable<? extends LCObject> accept(Map<String, Object> attributes) {
    LCUser currentUser = LCUser.currentUser();
    if (null == currentUser) {
      logger.d("current user is null.");
      return Observable.error(ErrorUtils.propagateException(LCException.SESSION_MISSING,
              "No valid session token, make sure signUp or login has been called."));
    }
    return currentUser.acceptFriendshipRequest(this, attributes);
  }

  /**
   * decline friend request by current user.
   *
   * @return observable instance
   */
  public Observable<? extends LCObject> decline() {
    LCUser currentUser = LCUser.currentUser();
    if (null == currentUser) {
      logger.d("current user is null.");
      return Observable.error(ErrorUtils.propagateException(LCException.SESSION_MISSING,
              "No valid session token, make sure signUp or login has been called."));
    }
    return currentUser.declineFriendshipRequest(this);
  }

  @Override
  public Observable<? extends LCObject> saveInBackground(final LCSaveOption option) {
    return Observable.error(ErrorUtils.propagateException(LCException.OPERATION_FORBIDDEN,
            "save operation isn't allowed in AVFriendshipRequest class."));
  }
}
