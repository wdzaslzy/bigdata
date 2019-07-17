package top.wdzaslzy.bigdata.util;

import java.io.IOException;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

/**
 * @author zhongyou_li
 */
public final class OkHttpUtil {

    private static final OkHttpClient okHttpClient = new OkHttpClient();

    public static Response sendGetRequest(String url) {
        Request request = new Request.Builder().url(url).build();
        Response response;
        try {
            response = okHttpClient.newCall(request).execute();
            if (response.isSuccessful()) {
                return response;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        throw new RuntimeException("GET请求发送失败：" + response.code() + ";" + response.message());
    }

    public static Response sendPostRequestForJson(String url, String jsonEntity) {
        RequestBody body = RequestBody
                .create(MediaType.parse("application/json; charset=utf-8"), jsonEntity);
        Request request = new Request.Builder()
                .url(url)
                .post(body)
                .build();
        Response response;
        try {
            response = okHttpClient.newCall(request).execute();
            if (response.isSuccessful()) {
                return response;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        throw new RuntimeException("POST请求发送失败：" + response.code() + ";" + response.message());
    }

}