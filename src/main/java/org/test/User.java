package org.test;

import org.hudi.ParquetTest;

import java.io.Serializable;
import java.util.Random;

public class User implements Serializable {
  private int id;
  private String name;
  private String address;
  private String city;
  private String company;
  private String path = "/user/hive/warehouse/iceberg_db.db/ods_fy_test/data/00000-0-15553a3a-fa92-4afe-956c-cb2b32569a06-00001.parquet";
//  private String des2 = "aaaaaaaaaaaaaaaaaaasdfffffffffffffffffffffffaweeeeeeeeeeeeee";
//  private String des3 = "aaaaaaaaaaaaaaaaaaasdfffffffffffffffffffffffaweeeeeeeeeeeeee";
//  private String des4 = "aaaaaaaaaaaaaaaaaaasdfffffffffffffffffffffffaweeeeeeeeeeeeee";
//  private String des5 = "aaaaaaaaaaaaaaaaaaasdfffffffffffffffffffffffaweeeeeeeeeeeeee";
//  private String des6 = "aaaaaaaaaaaaaaaaaaasdfffffffffffffffffffffffaweeeeeeeeeeeeee";
//  private String des7 = "aaaaaaaaaaaaaaaaaaasdfffffffffffffffffffffffaweeeeeeeeeeeeee";
//  private String des8 = "aaaaaaaaaaaaaaaaaaasdfffffffffffffffffffffffaweeeeeeeeeeeeee";
//  private Random random = new Random();
//  private static final String[] cities = {
//    "zhejiang" ,"anhui", "jiangsu", "shanghai", "hubei",
//    "shandong", "jiangxi", "guangdong","changsha","shenxi","neimenggu"};
//
//  private static final String[] companies = {
//    "阿里巴巴集团" ,"腾讯控股", "台积电", "鸿海精密", "小米集团",
//    "百度", "联发科", "京东","联想集团","海康威视","比亚迪"};

  public User() {}

  public User(int id, String key) {
    this.id = id;
    this.name = "name"+key;
    this.address = "asdasdfasdfadfa223343434address" + id;
    this.city = "zhejiangzhejiang";
    this.company = "阿里巴巴集团";
  }

  public User(int id) {
    String key = String.format(ParquetTest.localFormatter, id);
    this.id = id;
    this.name = "name"+key;
    this.address = "asdasdfasdfadfa223343434address" + id;
    this.city = "zhejiangzhejiang";
    this.company = "阿里巴巴集团";
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getAddress() {
    return address;
  }

  public void setAddress(String address) {
    this.address = address;
  }

  public String getCity() {
    return city;
  }

  public void setCity(String city) {
    this.city = city;
  }

  public String getCompany() {
    return company;
  }

  public String getPath() {
    return path;
  }

  public void setCompany(String company) {
    this.company = company;
  }

  @Override public String toString() {
    return "User{" + "id=" + id  + ", path='" + path + '\'' + '}';
  }

  public String getKey(){
    String key = String.format(ParquetTest.localFormatter, id);
    return key + this.name + this.address + this.city + this.company;
  }

  public static String getKey(int id){
    String key = String.format(ParquetTest.localFormatter, id);
    StringBuilder sb = new StringBuilder(key)
            .append("name")
            .append(key)
            .append("asdasdfasdfadfa223343434address")
            .append(id)
            .append("zhejiangzhejiang阿里巴巴集团")
            ;
    return sb.toString();
  }

  public static void main(String[] args) {
    Integer id = 1;
    String rowkey = new User(id).getKey();
    System.out.println(rowkey);
    System.out.println(getKey(id));
  }
}
