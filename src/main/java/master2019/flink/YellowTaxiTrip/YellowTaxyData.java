package master2019.flink.YellowTaxiTrip;

import java.util.Date;

public class YellowTaxyData {
  private int vendorid;
  private String tpeppickupdatetime;
  private String pickupyear;
  private String pickupmonth;
  private String pickupday;
  private String pickuphour;
  private String pickupminute;
  private String pickupsecond;
  private String tpepdropoffdatetime;
  private String dropoffyear;
  private String dropoffmonth;
  private String dropoffday;
  private String dropoffhour;
  private String dropoffminute;
  private String dropoffsecond;
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

    String[] splitPickupTimestamp = splitTimestamp(tpeppickupdatetime);
    this.pickupyear = splitPickupTimestamp[0];
    this.pickupmonth = splitPickupTimestamp[1];
    this.pickupday = splitPickupTimestamp[2];
    this.pickuphour = splitPickupTimestamp[3];
    this.pickupminute = splitPickupTimestamp[4];
    this.pickupsecond = splitPickupTimestamp[5];

    String[] splitDropOffTimestamp = splitTimestamp(tpepdropoffdatetime);
    this.dropoffyear = splitDropOffTimestamp[0];
    this.dropoffmonth = splitDropOffTimestamp[1];
    this.dropoffday = splitDropOffTimestamp[2];
    this.dropoffhour = splitDropOffTimestamp[3];
    this.dropoffminute = splitDropOffTimestamp[4];
    this.dropoffsecond = splitDropOffTimestamp[5];
  }

  public String[] splitTimestamp(String date) {
    String[] timestampSplit = date.split(" ");
    String datePart = timestampSplit[0];
    String timePart = timestampSplit[1];

    String[] datePartSplit = datePart.split("-");
    String[] timePartSplit = timePart.split(":");

    return new String[] {
      datePartSplit[0],
      datePartSplit[1],
      datePartSplit[2],
      timePartSplit[0],
      timePartSplit[1],
      timePartSplit[2]
    };
  }

  @Override
  public String toString() {
    return "("
        + this.getTpeppickupdatetime()
        + " || "
        + this.getPickupyear()
        + "+"
        + this.getPickupmonth()
        + "+"
        + this.getPickupday()
        + " <-> "
        + this.getPickuphour()
        + "+"
        + this.getPickupminute()
        + "+"
        + this.getPickupsecond()
        + " || "
        + this.getTpepdropoffdatetime()
        + " || "
        + this.getDropoffyear()
        + "+"
        + this.getDropoffmonth()
        + "+"
        + this.getDropoffday()
        + " <-> "
        + this.getDropoffhour()
        + "+"
        + this.getDropoffminute()
        + "+"
        + this.getDropoffsecond()
        + ")";
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

  public String getPickupyear() {
    return pickupyear;
  }

  public void setPickupyear(String pickupyear) {
    this.pickupyear = pickupyear;
  }

  public String getPickupmonth() {
    return pickupmonth;
  }

  public void setPickupmonth(String pickupmonth) {
    this.pickupmonth = pickupmonth;
  }

  public String getPickupday() {
    return pickupday;
  }

  public void setPickupday(String pickupday) {
    this.pickupday = pickupday;
  }

  public String getPickuphour() {
    return pickuphour;
  }

  public void setPickuphour(String pickuphour) {
    this.pickuphour = pickuphour;
  }

  public String getPickupminute() {
    return pickupminute;
  }

  public void setPickupminute(String pickupminute) {
    this.pickupminute = pickupminute;
  }

  public String getPickupsecond() {
    return pickupsecond;
  }

  public void setPickupsecond(String pickupsecond) {
    this.pickupsecond = pickupsecond;
  }

  public String getDropoffyear() {
    return dropoffyear;
  }

  public void setDropoffyear(String dropoffyear) {
    this.dropoffyear = dropoffyear;
  }

  public String getDropoffmonth() {
    return dropoffmonth;
  }

  public void setDropoffmonth(String dropoffmonth) {
    this.dropoffmonth = dropoffmonth;
  }

  public String getDropoffday() {
    return dropoffday;
  }

  public void setDropoffday(String dropoffday) {
    this.dropoffday = dropoffday;
  }

  public String getDropoffhour() {
    return dropoffhour;
  }

  public void setDropoffhour(String dropoffhour) {
    this.dropoffhour = dropoffhour;
  }

  public String getDropoffminute() {
    return dropoffminute;
  }

  public void setDropoffminute(String dropoffminute) {
    this.dropoffminute = dropoffminute;
  }

  public String getDropoffsecond() {
    return dropoffsecond;
  }

  public void setDropoffsecond(String dropoffsecond) {
    this.dropoffsecond = dropoffsecond;
  }
}
