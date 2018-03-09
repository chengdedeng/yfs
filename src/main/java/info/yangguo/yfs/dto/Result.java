package info.yangguo.yfs.dto;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.io.Serializable;

/**
 * @author:杨果
 * @date:15/12/21 下午2:24
 * <p/>
 * Description:
 */
@ApiModel(value = "Rest result", description = "请求结果")
public class Result<T> implements Serializable {
    private static final long serialVersionUID = -4696898674758059398L;
    @ApiModelProperty(value = "结果代码")
    private int code;
    @ApiModelProperty(value = "结果对象")
    private T value;


    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }
}
