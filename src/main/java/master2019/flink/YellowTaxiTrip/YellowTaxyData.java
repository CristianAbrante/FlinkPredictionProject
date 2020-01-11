package master2019.flink.YellowTaxiTrip;

import java.util.Date;

public class YellowTaxyData {
  private int vendorid;
  private String tpeppickupdatetime;
  private String tpepdropoffdatetime;
  private int passengercount;
  private double tripdistance;
  private int ratecodeid;
  private String storeandfwdflag;
  private int pulocationid;
  private int dolocationid;
  private int paymenttype;
  private double fareamount;
  private double extra;
  private double mtatax;
  private double tipamount;
  private double tollsamount;
  private double improvementsurcharge;
  private double totalamount;
  private double congestionamount;

  public YellowTaxyData() {}

  public YellowTaxyData(
      int vendorid,
      String tpeppickupdatetime,
      String tpepdropoffdatetime,
      int passengercount,
      double tripdistance,
      int ratecodeid,
      String storeandfwdflag,
      int pulocationid,
      int dolocationid,
      int paymenttype,
      double fareamount,
      double extra,
      double mtatax,
      double tipamount,
      double tollsamount,
      double improvementsurcharge,
      double totalamount,
      double congestionamount) {
    this.vendorid = vendorid;
    this.tpeppickupdatetime = tpeppickupdatetime;
    this.tpepdropoffdatetime = tpepdropoffdatetime;
    this.passengercount = passengercount;
    this.tripdistance = tripdistance;
    this.ratecodeid = ratecodeid;
    this.storeandfwdflag = storeandfwdflag;
    this.pulocationid = pulocationid;
    this.dolocationid = dolocationid;
    this.paymenttype = paymenttype;
    this.fareamount = fareamount;
    this.extra = extra;
    this.mtatax = mtatax;
    this.tipamount = tipamount;
    this.tollsamount = tollsamount;
    this.improvementsurcharge = improvementsurcharge;
    this.totalamount = totalamount;
    this.congestionamount = congestionamount;
  }

  public Date getDate() {
    return new Date("15-03-1997");
  }

  public int getVendorid() {
    return vendorid;
  }

  public void setVendorid(int vendorid) {
    this.vendorid = vendorid;
  }

  public String getTpeppickupdatetime() {
    return tpeppickupdatetime;
  }

  public void setTpeppickupdatetime(String tpeppickupdatetime) {
    this.tpeppickupdatetime = tpeppickupdatetime;
  }

  public String getTpepdropoffdatetime() {
    return tpepdropoffdatetime;
  }

  public void setTpepdropoffdatetime(String tpepdropoffdatetime) {
    this.tpepdropoffdatetime = tpepdropoffdatetime;
  }

  public int getPassengercount() {
    return passengercount;
  }

  public void setPassengercount(int passengercount) {
    this.passengercount = passengercount;
  }

  public double getTripdistance() {
    return tripdistance;
  }

  public void setTripdistance(double tripdistance) {
    this.tripdistance = tripdistance;
  }

  public int getRatecodeid() {
    return ratecodeid;
  }

  public void setRatecodeid(int ratecodeid) {
    this.ratecodeid = ratecodeid;
  }

  public String getStoreandfwdflag() {
    return storeandfwdflag;
  }

  public void setStoreandfwdflag(String storeandfwdflag) {
    this.storeandfwdflag = storeandfwdflag;
  }

  public int getPulocationid() {
    return pulocationid;
  }

  public void setPulocationid(int pulocationid) {
    this.pulocationid = pulocationid;
  }

  public int getDolocationid() {
    return dolocationid;
  }

  public void setDolocationid(int dolocationid) {
    this.dolocationid = dolocationid;
  }

  public int getPaymenttype() {
    return paymenttype;
  }

  public void setPaymenttype(int paymenttype) {
    this.paymenttype = paymenttype;
  }

  public double getFareamount() {
    return fareamount;
  }

  public void setFareamount(double fareamount) {
    this.fareamount = fareamount;
  }

  public double getExtra() {
    return extra;
  }

  public void setExtra(double extra) {
    this.extra = extra;
  }

  public double getMtatax() {
    return mtatax;
  }

  public void setMtatax(double mtatax) {
    this.mtatax = mtatax;
  }

  public double getTipamount() {
    return tipamount;
  }

  public void setTipamount(double tipamount) {
    this.tipamount = tipamount;
  }

  public double getTollsamount() {
    return tollsamount;
  }

  public void setTollsamount(double tollsamount) {
    this.tollsamount = tollsamount;
  }

  public double getImprovementsurcharge() {
    return improvementsurcharge;
  }

  public void setImprovementsurcharge(double improvementsurcharge) {
    this.improvementsurcharge = improvementsurcharge;
  }

  public double getTotalamount() {
    return totalamount;
  }

  public void setTotalamount(double totalamount) {
    this.totalamount = totalamount;
  }

  public double getCongestionamount() {
    return congestionamount;
  }

  public void setCongestionamount(double congestionamount) {
    this.congestionamount = congestionamount;
  }
}
