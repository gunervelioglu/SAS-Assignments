/*	Guner Velioglu	--	61441902	*/

/***	ASSIGNMENT 2	***/

/* Log in and download Compustat - Funda and Fundq data from WRDS. */

%let wrds = wrds.wharton.upenn.edu 4016;options comamid = TCP remote=WRDS;
signon username=_prompt_;
/*	Downloading the data part of the code is based on the code we have seen in week 2 lecture.	*/
rsubmit;
libname myfiles "/home/ufl/guner";
proc sql;
	create table myfiles.a_funda as
		select gvkey, fyear, datadate, sich, sale
	  	from comp.funda 
  	where 1980 <= fyear <= 2013 and indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C' ;
quit;

proc sort data=myfiles.a_funda nodupkey; by gvkey fyear;run;

data myfiles.a_funda;
	set myfiles.a_funda;
	key = gvkey || fyear;
run;
proc download data=myfiles.a_funda out=a_funda;run;
endrsubmit;

rsubmit;
libname myfiles "/home/ufl/guner";
proc sql;
	create table myfiles.b_fundq as
	select a.key, b.gvkey, b.fyearq, b.datadate, b.fqtr 
	from myfiles.a_funda a, comp.fundq b
	where a.gvkey = b.gvkey and a.fyear = b.fyearq;
quit;
proc download data=myfiles.b_fundq out=b_fundq;run;
endrsubmit;

/***	Question 1	***/

data b_fundq (keep = gvkey fyearq datadate fqtr month_qtr); set b_fundq; month_qtr = month(datadate);run;
proc sort data = b_fundq; by fyearq month_qtr; run;

/*	Count quarter ends for each month.	*/
proc sql;
	create table mean_quarter_ends as
	select month_qtr, count(*) as month_qtr_N from b_fundq group by month_qtr;

/*	Sum quarter counts for all months.	*/
proc sql;
  create table mean_quarter_ends2 as
  select sum(month_qtr_n) as month_qtr_N_Sum from mean_quarter_ends;

proc sql;
	create table quarter_means as
	select a.month_qtr, a.month_qtr_N, b.month_qtr_N_Sum
	from mean_quarter_ends a, mean_quarter_ends2 b;

/*	Find the ratio of monthly quarter counts / total quarter counts.	*/
data quarter_means (keep = month_qtr monthly_average_quarters); set quarter_means; monthly_average_quarters = month_qtr_N / month_qtr_N_Sum; run;

/***	Question 2	***/

/*	First part of the following code is based on the example on the website.	*/
rsubmit;
libname segments "/wrds/comp/sasdata/naa/segments_current";
data b_segm_geo (keep = GVKEY datadate STYPE SID IAS CAPXS NAICS NAICSH NAICSS1 NAICSS2 NIS OPS SALES SICS1 SICS2 SNMS SOPTP1 INTSEG);
set segments.Wrds_segmerged;
if srcdate eq datadate;	/* prevent duplicates: use the data when first published (not later years)*/
if stype IN ("GEOSEG");	/* select geographic segments */
if SICS1 ne "";	/* keep segments that have SIC industry code */
if sales > 0;	/* keep segments with positive sales */
run;
proc download data=b_segm_geo out=b_segm_geo;run;
endrsubmit;

proc sql;
  create table c_joined as
  select a.gvkey, a.fyear, a.datadate, b.sid, b.SICS1 as sics
  from a_funda a, b_segm_geo b
  where a.gvkey = b.gvkey and a.datadate = b.datadate;
 
proc sort data=c_joined nodupkey; by gvkey fyear sid;run;	

/* Count the geographical segments for each year. */
proc sql;
  create table d_count (keep = gvkey fyear numgeosegs) as
  select distinct gvkey, fyear, sid, count(*) as numGeoSegs from c_joined
  group by gvkey, fyear;
proc sort data=d_count nodupkey; by gvkey fyear;run;

/*	For each year, count the number of firms categorized by the number of geographical segments they have.	*/
proc sql;
  create table seg_firm_count (keep = fyear numgeosegs numfirms) as
  select distinct fyear, numgeosegs, count(*) as numfirms from d_count
  group by fyear, numgeosegs;

/***	Question 3	***/

rsubmit;
libname segments "/wrds/comp/sasdata/naa/segments_current";
data b_segm_bus (keep = GVKEY datadate STYPE SID IAS CAPXS NAICS NAICSH NAICSS1 NAICSS2 NIS OPS SALES SICS1 SICS2 SNMS SOPTP1 INTSEG);
set segments.Wrds_segmerged;
if srcdate eq datadate;	/* prevent duplicates: use the data when first published (not later years)*/
if stype IN ("BUSSEG", "OPSEG");	/* select business/operating segments */
if SICS1 ne "";	/* keep segments that have SIC industry code */
if sales > 0;	/* keep segments with positive sales */
run;
proc download data=b_segm_bus out=b_segm_bus; run;
endrsubmit;

proc sql;
  create table e_joined as
  select a.gvkey, a.fyear, a.datadate, a.sich, b.sid, b.SICS1 as sics
  from a_funda a, b_segm_bus b
  where a.gvkey = b.gvkey and a.datadate = b.datadate;
run;

proc sort data=e_joined nodupkey; by gvkey fyear sid;run;	

/*	Count the business segments for each year. */
proc sql;
  create table f_count as
  select distinct gvkey, fyear, sid, count(*) as numBusSegs from e_joined
  group by gvkey, fyear;
run;

/*	Find the firms with only 1 business segment and bring their industry classification codes.	*/
proc sql;
  create table g_joined as
  select a.gvkey, a.fyear, a.sid, a.numBusSegs, b.sics, b.sich
  from f_count a, e_joined b
  where a.gvkey = b.gvkey and a.fyear = b.fyear and a.sid = b.sid and numBusSegs = 1;
 run;

data g_joined; set g_joined;  
	if sich = sics then sic_match = 1; 
	if sic_match ne 1 then sic_match = 0; 
run;

proc sql;
  create table match_percentage as
  select mean(sic_match) as match_ratio from g_joined;
run;

/***	Question 4	***/

proc sql;
  create table industry_sales as
  select distinct sich, sum(sale) as lifetime_sales, count(sich) as number_of_firms from a_funda
  where sale ne . and sich ne .
group by sich;
delete from industry_sales where number_of_firms < 20;	/*	Delete the industries with less than 20 firms.	*/

/***	Question 5	***/

data returns;
  input @01 id       
  	@03 date  MMDDYY10.
        @14 return;
format date date9.;
datalines;
1 10/31/2013 0.01
1 11/30/2013 0.02
1 12/31/2013 0.03
1 01/31/2014 -0.01
1 02/28/2014 0.01
2 10/31/2013 -0.01
2 11/30/2013 0.02
2 12/31/2013 0.01
2 01/31/2014 -0.02
2 02/28/2014 -0.03
2 03/31/2014 0.02 
run;
 
data yearly;
  input @01 id        
  	@03 date  MMDDYY10.
        @14 equity;
format date date9.;
datalines;
1 12/31/2011 8
1 12/31/2012 10
1 12/31/2013 11
2 12/31/2012 30
2 12/31/2013 28
run;

proc sql;
  create table book_value_matched as
  select a.*, b.equity
  from returns a, yearly b
  where a.id = b.id and a.date >= b.date
group by a.id, a.date
having b.date = max(b.date);

/*  The above code firstly matches all the prior book values, such that they would not exceed the return date.	*/
/*	It then keeps the latest book value, which is most relevant for that return date.	*/
