/*	Guner Velioglu	--	61441902	*/

/***	ASSIGNMENT 3	***/

/* Log in and download data from WRDS. */
%let wrds = wrds.wharton.upenn.edu 4016;options comamid = TCP remote=WRDS;
signon username=_prompt_;

rsubmit;
/*	Firstly, download the list of firms with their sich.	*/
libname myfiles "~";
proc sql;
	create table myfiles.a_funda as
		select gvkey, fyear, datadate, sich
	  	from comp.funda 
  	where 		
		2006 <= fyear <= 2009
	and indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C' 
	and 6000 <= sich <= 6499 ;	/*	We are interested in financial industry firms for the moment. (Ref: http://www.wrds.us/index.php/tutorial/view/compustat_compute_roe)	*/
quit;

/*	Drop doubles.	*/
proc sort data=myfiles.a_funda nodupkey; by gvkey fyear;run;
proc download data=myfiles.a_funda out=a_funda;run;
endrsubmit;

rsubmit;
/*	Match the firms with their permno, using compustat-crsp merged.	*/
libname myfiles "~";
proc sql; 
  create table ccMerged as 
  select a.*, b.lpermno as permno
  from myfiles.a_funda a left join crsp.ccmxpf_linktable b 
    on a.gvkey = b.gvkey 
    and b.lpermno ne . 
    and b.linktype in ("LC" "LN" "LU" "LX" "LD" "LS") 
    and b.linkprim IN ("C", "P")  
    and ((a.datadate >= b.LINKDT) or b.LINKDT eq .B) and  
       ((a.datadate <= b.LINKENDDT) or b.LINKENDDT eq .E)   ; 
  quit; 
  
proc download data=ccMerged out=u_withpermno; run;
endrsubmit; 
rsubmit;
/*	Find the monthly returns for each firm.	*/
proc sql;
	create table _return as 
	select a.*, b.ret, b.date
	from ccmerged a, crspa.msf b
	where a.permno = b.permno
	and b.date <= a.datadate <= b.date + 360
	and b.ret ne . ;
quit;
proc sort data = _return nodupkey; by gvkey date; run;
proc download data= _return out=b_return; run;
endrsubmit;
rsubmit;
/*	Find the index returns associated with each month.	*/
proc sql;
	create table c_return as 
	select a.*, b.caldt, b.vwretd	/*	I bring caldt (calendar date), to make sure it matches 1 to 1.	*/
	from _return a, crspa.msix b
	where b.caldt = a.date;
quit;
proc sort data = c_return nodupkey; by gvkey caldt; run;
proc download data= c_return out=c_return; run;
endrsubmit;

/*	Question 1	*/

/*	Find the excess return for each month	*/
data exc_return; set c_return;
	excess_return = ret - vwretd;
	excess_return1 = excess_return + 1;
run;

/*	Calculate the yearly abnormal return. I assume the formula to be: product(1+excess return_(month))-1, month=1,...,12.	*/
proc sql ;
	create table abnormal_return as
	select gvkey, fyear, exp(sum(log(excess_return1)))-1 as abnormal_return from exc_return
 	group by gvkey, fyear;
quit;

/*	Question 2	*/

/*	Construct the starting dataset.	*/
data funda_permno; set u_withpermno;
	enddate = datadate + 360;
	where permno ne .;
run;

%macro getReturn(dsin=, dsout=, start=, end=);

rsubmit;
/*	Download the monthly returns from crsp.	*/
proc sql;
	create table myfiles.b_crsp as
	select date, ret, permno
	from crspa.msf
	where ret ne .; quit;
	proc download data = myfiles.b_crsp out=b_crsp; run;
endrsubmit;
/*	Merge the monthly returns to starting dataset.	*/
proc sql;
create table step1 as 
	select a.*, b.ret, b.date
	from &dsin a, b_crsp b
	where a.permno = b.permno
	and &start <= b.date + 360 <= &end	
	and b.ret ne . ;
rsubmit;
/*	Download the index returns.	*/
proc sql;
	create table myfiles.index_return as 
	select caldt, vwretd
	from crspa.msix;
	quit;
	proc download data = myfiles.index_return out=index_return; run;
endrsubmit;
/*	Merge the index returns with the main dataset.	*/
create table step2 as 
	select a.*, b.caldt, b.vwretd
	from step1 a, index_return b
	where a.date = b.caldt;
proc sort data = step2 nodupkey; by gvkey caldt; run;
/*	Calculate the abnormal return.	*/
data exc_return; set step2;
	excess_return = ret - vwretd;
	excess_return1 = excess_return + 1;
run;
proc sql ;
	create table &dsout as
	select gvkey, fyear, exp(sum(log(excess_return1)))-1 as abnormal_return from exc_return
 	group by gvkey, fyear;
quit;

%mend;
%getReturn(dsin=funda_permno, dsout=funda_abnormal, start=datadate, end=enddate);	/*	Compare funda_abnormal(Q2) with abnormal_return(Q1) for consistency. It matches!	*/

/*	Question 3	*/

/*	Construct the starting dataset	*/

rsubmit;
libname myfiles "~";
proc sql;
	create table myfiles.s_funda as
		select gvkey, fyear, datadate
	  	from comp.funda 
  	where indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C'; 
quit;
/*	Drop doubles.	*/
proc sort data=myfiles.s_funda nodupkey; by gvkey fyear;run;
proc download data=myfiles.s_funda out=s_funda;run;
endrsubmit;	

%macro getMarketVars(dsin=, dsout=);
/*	Match the firms with their permno, using compustat-crsp merged.	*/
rsubmit;
libname myfiles "~";
proc sql; 
  create table cclink as 
  select gvkey, lpermno as permno, linktype, linkprim, linkdt, linkenddt
  from crsp.ccmxpf_linktable; 
  quit;  
proc download data=cclink out=cclink; run;
endrsubmit; 
proc sql;
create table skw1 as 
	select a.*, b.permno	
from &dsin a left join cclink b 
    on a.gvkey = b.gvkey 
    and b.permno ne . 
    and b.linktype in ("LC" "LN" "LU" "LX" "LD" "LS") 
    and b.linkprim IN ("C", "P")  
    and ((a.datadate >= b.LINKDT) or b.LINKDT eq .B) and  
       ((a.datadate <= b.LINKENDDT) or b.LINKENDDT eq .E); 
quit; 
proc sort data=skw1 nodupkey; by gvkey fyear;run;
data skw1; set skw1; where permno ne . ; run;
/*	Download the daily return and price data.	*/
rsubmit;
proc sql;
	create table daily_prices as
	select permno, date, bid, ask, ret
	from crspa.dsf b
	where and permno ne .
	/*	and 2000 < year(date) < 2003	*/;		/*	Uncomment this line for a smaller daily return file.	*/
quit;
proc sort data=daily_prices nodupkey; by permno date; run;
proc download data=daily_prices out=daily_prices; run;
endrsubmit;

/*	Match the daily information according to permno-datadate since fiscal year may not be the most relevant for some dates.	*/
proc sql;
create table skw2 as 
	select a.*, b.permno, b.date, b.bid, b.ask, b.ret	
from skw1 a left join daily_prices b 
    on a.permno = b.permno
	and a.datadate - 360 <= b.date <= a.datadate; 
quit; 
proc sort data = skw2 nodupkey; by permno date; run;

/*	Find the average bid-ask spread.	*/
data skw3; set skw2; spread = ask - bid; where date ne .; run;
proc sort data = skw3; by gvkey datadate;	run;
proc means data = skw3 noprint;
output out = skw4 mean= spread_mean;
var spread;
by gvkey datadate;
run;
/*	Find the standard deviation and skewness of daily returns.	*/
data skw3; set skw3; where -10000< ret < 10000; run;

proc means data = skw3 noprint;
output out = skw5 std= return_std skewness= return_skewness;
var ret;
by gvkey datadate;
run;

/*	Merge those statistics.	*/
proc sql;
	create table skw6 as
	select a.gvkey, a.datadate, a.spread_mean, b.return_std, b.return_skewness
	from skw4 a left join skw5 b
	on a.gvkey = b.gvkey
	and a.datadate = b.datadate;
	quit;
proc sort data = skw6 nodupkey; by gvkey datadate; run;
data &dsout; set skw6; run;

%mend;
%getMarketVars(dsin=s_funda, dsout=funda_market_vars)
