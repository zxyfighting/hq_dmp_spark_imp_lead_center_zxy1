package com.faw.hq.dmp.spark.imp.thread.bean;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * @auth:zhangxiuyun
 * @purpose:对象封装
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ThreadBean {
    private String leadId;         //线索ID，清洗前的线索id
    private String createTime;     //线索创建中心
    private String dealerId;       //经销商编码
    private String dealerName;     //经销商名称
    private String countyCode;     //经销商区县编码
    private String countyName;     //经销商区县名称
    private String cityCode;       //经销商地市编码
    private String cityName;       //经销商地市名称
    private String provinceCode;   //省份编码
    private String provinceName;   //省份名称
    private String regionCode;     //战区编码
    private String regionName;     //战区名称
    private String seriesCode;     //车系编码
    private String seriesName;     //车系名称
    private String vfrom1;         //线索来源1
    private String vfrom2;         //线索来源2
    private String vfrom3;         //线索来源3
    private String vfrom4;         //线索来源4
    private Boolean newLeadHQ;      //是否红旗内新线索
    private Boolean newLeadDealer;  //是否本店内新线索

    public String getLeadId() {
        return leadId;
    }

    public void setLeadId(String leadId) {
        this.leadId = leadId;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getDealerId() {
        return dealerId;
    }

    public void setDealerId(String dealerId) {
        this.dealerId = dealerId;
    }

    public String getDealerName() {
        return dealerName;
    }

    public void setDealerName(String dealerName) {
        this.dealerName = dealerName;
    }

    public String getCountyCode() {
        return countyCode;
    }

    public void setCountyCode(String countyCode) {
        this.countyCode = countyCode;
    }

    public String getCountyName() {
        return countyName;
    }

    public void setCountyName(String countyName) {
        this.countyName = countyName;
    }

    public String getCityCode() {
        return cityCode;
    }

    public void setCityCode(String cityCode) {
        this.cityCode = cityCode;
    }

    public String getCityName() {
        return cityName;
    }

    public void setCityName(String cityName) {
        this.cityName = cityName;
    }

    public String getProvinceCode() {
        return provinceCode;
    }

    public void setProvinceCode(String provinceCode) {
        this.provinceCode = provinceCode;
    }

    public String getProvinceName() {
        return provinceName;
    }

    public void setProvinceName(String provinceName) {
        this.provinceName = provinceName;
    }

    public String getRegionCode() {
        return regionCode;
    }

    public void setRegionCode(String regionCode) {
        this.regionCode = regionCode;
    }

    public String getRegionName() {
        return regionName;
    }

    public void setRegionName(String regionName) {
        this.regionName = regionName;
    }

    public String getSeriesCode() {
        return seriesCode;
    }

    public void setSeriesCode(String seriesCode) {
        this.seriesCode = seriesCode;
    }

    public String getSeriesName() {
        return seriesName;
    }

    public void setSeriesName(String seriesName) {
        this.seriesName = seriesName;
    }

    public String getVfrom1() {
        return vfrom1;
    }

    public void setVfrom1(String vfrom1) {
        this.vfrom1 = vfrom1;
    }

    public String getVfrom2() {
        return vfrom2;
    }

    public void setVfrom2(String vfrom2) {
        this.vfrom2 = vfrom2;
    }

    public String getVfrom3() {
        return vfrom3;
    }

    public void setVfrom3(String vfrom3) {
        this.vfrom3 = vfrom3;
    }

    public String getVfrom4() {
        return vfrom4;
    }

    public void setVfrom4(String vfrom4) {
        this.vfrom4 = vfrom4;
    }

    public Boolean getNewLeadHQ() {
        return newLeadHQ;
    }

    public void setNewLeadHQ(Boolean newLeadHQ) {
        this.newLeadHQ = newLeadHQ;
    }

    public Boolean getNewLeadDealer() {
        return newLeadDealer;
    }

    public void setNewLeadDealer(Boolean newLeadDealer) {
        this.newLeadDealer = newLeadDealer;
    }

    public static String getHive(){
        StringBuilder sb = new StringBuilder();
        sb.append("\\N;").append("\\N;").append("\\N;").append("\\N;").append("\\N;").append("\\N;").append("\\N;")
                .append("\\N;").append("\\N;").append("\\N;").append("\\N;").append("\\N;").append("\\N;").append("\\N;")
                .append("\\N;").append("\\N;").append("\\N;").append("\\N;").append("\\N;").append("\\N");
        return sb.toString();
    }
    public String jsonToLine() {
        StringBuilder sb = new StringBuilder();
        if (leadId==null||leadId.trim().length()==0) {
            sb.append("\\N;");
        } else {
            sb.append(leadId).append(";");
        }
        if (createTime==null||createTime.trim().length()==0) {
            sb.append("\\N;");
        } else {
            sb.append(createTime).append(";");
        }
        if (dealerId==null||dealerId.trim().length()==0) {
            sb.append("\\N;");
        } else {
            sb.append(dealerId).append(";");
        }
        if (dealerName==null||dealerName.trim().length()==0) {
            sb.append("\\N;");
        } else {
            sb.append(dealerName).append(";");
        }
        if (countyCode==null||countyCode.trim().length()==0) {
            sb.append("\\N;");
        } else {
            sb.append(countyCode).append(";");
        }
        if (countyName==null||countyName.trim().length()==0) {
            sb.append("\\N;");
        } else {
            sb.append(countyName).append(";");
        }
        if (cityCode==null||cityCode.trim().length()==0) {
            sb.append("\\N;");
        } else {
            sb.append(cityCode).append(";");
        }if (cityName==null||cityName.trim().length()==0) {
            sb.append("\\N;");
        } else {
            sb.append(cityName).append(";");
        }if (provinceCode==null||provinceCode.trim().length()==0) {
            sb.append("\\N;");
        } else {
            sb.append(provinceCode).append(";");
        }if (provinceName==null||provinceName.trim().length()==0) {
            sb.append("\\N;");
        } else {
            sb.append(provinceName).append(";");
        }if (regionCode==null||regionCode.trim().length()==0) {
            sb.append("\\N;");
        } else {
            sb.append(regionCode).append(";");
        }if (regionName==null||regionName.trim().length()==0) {
            sb.append("\\N;");
        } else {
            sb.append(regionName).append(";");
        }if (seriesCode==null||seriesCode.trim().length()==0) {
            sb.append("\\N;");
        } else {
            sb.append(seriesCode).append(";");
        }if (seriesName==null||seriesName.trim().length()==0) {
            sb.append("\\N;");
        } else {
            sb.append(seriesName).append(";");
        }if (vfrom1==null||vfrom1.trim().length()==0) {
            sb.append("\\N;");
        } else {
            sb.append(vfrom1).append(";");
        }if (vfrom2==null||vfrom2.trim().length()==0) {
            sb.append("\\N;");
        } else {
            sb.append(vfrom2).append(";");
        }if (vfrom3==null||vfrom3.trim().length()==0) {
            sb.append("\\N;");
        } else {
            sb.append(vfrom3).append(";");
        }if (vfrom4==null||vfrom4.trim().length()==0) {
            sb.append("\\N;");
        } else {
            sb.append(vfrom4).append(";");
        }if (newLeadHQ==null) {
            sb.append("\\N;");
        } else {
            sb.append(newLeadHQ).append(";");
        }if (newLeadDealer==null) {
            sb.append("\\N");
        } else {
            sb.append(newLeadDealer);
        }

        return sb.append(";").append("lead_center").toString();
    }

}
