package info.yangguo.yfs.config;

import io.netty.handler.codec.http.HttpHeaderNames;

public class StandardHeaders {
    /**
     * 指定该Object被下载时的网页的缓存行为
     */
    public static final String CACHE_CONTROL = HttpHeaderNames.CACHE_CONTROL.toString();
    /**
     * 指定该Object被下载时的名称
     */
    public static final String CONTENT_DISPOSITION = HttpHeaderNames.CONTENT_DISPOSITION.toString();
    /**
     * 指定该Object被下载时的内容编码格式
     */
    public static final String CONTENT_ENCODING = HttpHeaderNames.CONTENT_ENCODING.toString();
    /**
     * 指定该Object被下载时的内容语言编码
     */
    public static final String CONTENT_LANGUAGE = HttpHeaderNames.CONTENT_LANGUAGE.toString();
    /**
     * 过期时间
     */
    public static final String EXPIRES = HttpHeaderNames.EXPIRES.toString();
    /**
     * 该Object大小
     */
    public static final String CONTENT_LENGTH = HttpHeaderNames.CONTENT_LENGTH.toString();
    /**
     * 该Object文件类型
     */
    public static final String CONTENT_TYPE = HttpHeaderNames.CONTENT_TYPE.toString();
    /**
     * 最近修改时间
     */
    public static final String LAST_MODIFIED = HttpHeaderNames.LAST_MODIFIED.toString();
}
