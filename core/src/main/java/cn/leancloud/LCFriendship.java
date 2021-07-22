package cn.leancloud;

import cn.leancloud.annotation.LCClassName;
import cn.leancloud.utils.ErrorUtils;
import io.reactivex.Observable;

@LCClassName("_Followee")
public class LCFriendship extends LCObject {
  public static final String CLASS_NAME = "_Followee";

  public static final String ATTR_FOLLOWEE = "followee";// user who was followed by other
  public static final String ATTR_FOLLOWER = "follower";// user who followed someone.
  public static final String ATTR_USER = "user";
  public static final String ATTR_FRIEND_STATUS = "friendStatus";

  public LCFriendship() {
    super(CLASS_NAME);
  }

  public LCFriendship(LCObject object) {
    super(object);
    setClassName(CLASS_NAME);
  }

  public LCUser getFollowee() {
    return getLCObject(ATTR_FOLLOWEE);
  }

  public void setFollowee(LCUser followee) {
    put(ATTR_FOLLOWEE, followee);
  }

  public LCUser getFollower() {
    return getLCObject(ATTR_FOLLOWER);
  }

  public void setFollower(LCUser follower) {
    put(ATTR_FOLLOWER, follower);
  }

  /**
   * save friendship in backgound.
   * @param option save option.
   * @return observable instance.
   */
  @Override
  public Observable<? extends LCObject> saveInBackground(final LCSaveOption option) {
    LCUser currentUser = LCUser.currentUser();
    if (null == currentUser) {
      return Observable.error(ErrorUtils.propagateException(LCException.SESSION_MISSING,
              "No valid session token, make sure signUp or login has been called."));
    }
    return currentUser.updateFriendship(this);
  }
}
