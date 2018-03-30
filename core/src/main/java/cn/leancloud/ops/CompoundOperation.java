package cn.leancloud.ops;

import cn.leancloud.AVObject;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.*;

public class CompoundOperation extends BaseOperation {
  private List<ObjectFieldOperation> operations = new LinkedList<ObjectFieldOperation>();
  public CompoundOperation(String field) {
    super("Compound", field, null, false);
  }
  public CompoundOperation(String field, ObjectFieldOperation... ops) {
    this(field);
    operations.addAll(Arrays.asList(ops));
  }

  public Object apply(Object obj) {
    for (ObjectFieldOperation op: operations) {
      obj = op.apply(obj);
    }
    return obj;
  }

  protected ObjectFieldOperation mergeWithPrevious(ObjectFieldOperation previous) {
    operations.add(previous);
    return this;
  }

  public List<Map<String, Object>> encodeRestOp(AVObject parent) {
    List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
    if (null == parent) {
      return result;
    }
    String requestEndPoint = parent.getRequestRawEndpoint();
    String requestMethod = parent.getRequestMethod();
    for (int i = 1; i < this.operations.size(); i++) {
      ObjectFieldOperation tmp = this.operations.get(i);
      Map<String, Object> tmpOp = tmp.encode();
      tmpOp.put(KEY_INTERNAL_ID, parent.getObjectId());

      Map<String, Object> tmpResult = new HashMap<String, Object>();
      tmpResult.put(KEY_BODY, tmpOp);
      tmpResult.put(KEY_PATH, requestEndPoint);
      tmpResult.put(KEY_HTTP_METHOD, requestMethod);
      result.add(tmpResult);
    }
    return result;
  }

  public Map<String, Object> encode() {
    if (this.operations.size() < 1) {
      return null;
    }
    // just return the first Operation.
    return this.operations.get(0).encode();
  }
}
