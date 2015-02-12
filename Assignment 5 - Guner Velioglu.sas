/*	Download the macros.	*/
filename m1 url 'http://www.wrds.us/macros/array_functions.sas';
%include m1;
filename m2 url 'http://www.wrds.us/macros/runquit.sas';
%include m2;

/*	Guner Velioglu	--	61441902	*/

/***	ASSIGNMENT 5	***/

/* Login to WRDS. */
rsubmit;endrsubmit;
%let wrds = wrds.wharton.upenn.edu 4016;options comamid = TCP remote=WRDS;
signon username=_prompt_;

/***	Question 1	***/

%macro getFirmYears(dsout=);
rsubmit;
/*	Firstly, download the list of firms with their sich.	*/
proc sql;
	create table a_funda as
		select gvkey, fyear, datadate, sich
	  	from comp.funda 
  	where 2004 <= fyear <= 2013
	and indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C'
	and sich ne .;	/*	We need sich to be non-missing to say something about the firms.	*/
quit;
proc sort data=a_funda nodupkey; by gvkey fyear;run;
proc download data=a_funda out=a_funda;run;
endrsubmit;
rsubmit;
proc sql;
	create table b_fundq as
		select gvkey, fyearq, datadate, rdq, fqtr, fyr, saleq
		from comp.fundq
		where indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C'
		and fyr = 12; quit;
proc sort data = b_fundq nodupkey; by gvkey fyearq fqtr; run;
proc download data = b_fundq out = b_fundq; run;
endrsubmit;

proc sql;
	create table c_merged as
	select a.*,b.* from a_funda a, b_fundq b
	where a.gvkey = b.gvkey and a.fyear = b.fyearq and saleq ne .; 
quit;
proc sort data = c_merged nodupkey; by sich gvkey fyearq fqtr; run;

proc means data = c_merged noprint; output out=c_merged_test n= /autoname; var saleq; by sich gvkey; run;
proc means data = c_merged_test noprint; output out = c_merged_test_2 n= /autoname; var saleq_n; by sich; run;
proc means data = c_merged noprint; output out = c_merged_test_3 median= /autoname; var saleq; by sich; run;
proc sql;
	create table c_merged_numfirms as
	select a.*, b.saleq_n_n as numfirms
	from c_merged a left join c_merged_test_2 b
	on a.sich = b.sich;
quit;
proc sql;
	create table c_merged_numfirms_median as
	select a.*, b.saleq_median
	from c_merged_numfirms a left join c_merged_test_3 b
	on a.sich = b.sich;
quit;
proc sort data = c_merged_numfirms_median nodupkey; by sich gvkey fyearq fqtr; run;

data &dsout; set c_merged_numfirms_median; 
	if saleq >= saleq_median then above_median = 1; 
		else above_median = 0; 
	where numfirms > 9;
run;
%mend getfirmyears;
/*	Test macro	*/
%getFirmYears(dsout=industry_sales);

/***	Question 2	***/

%macro industryLeaders(dsin=,dsout=);
proc sort data=&dsin; by sich fyearq fqtr rdq; run;
proc sql;
	create table first_reporter as
	select sich, fyearq, fqtr, min(rdq) as earliest_report from &dsin
	group by sich, fyearq, fqtr
	having above_median = 1;
quit;
proc sql;
	create table report_delay as
	select a.*, b.earliest_report
	from &dsin a, first_reporter b
	where a.sich = b.sich and a.fyearq = b.fyearq and a.fqtr = b.fqtr;
quit;
data report_delay_2; set report_delay;
	delay = rdq - earliest_report;
	where rdq ne .;	/*	I drop if report date is missing, because I don't know whether it will be the first or last reporting company in that case.	*/
run;
proc rank data = report_delay_2 out=report_delay_3 ties=low groups=10;
	by sich fyearq fqtr;
	var delay;
	ranks delay_rank;
run;
/*	Data is ranked from 0 to 9, I change it from 1 to 10	*/
data report_delay_4; set report_delay_3;
	delay_rank = delay_rank + 1;
run;
proc means data=report_delay_4 noprint; 
	output out= report_delay_5 max(delay_rank)= rank_max;
	var delay_rank;
	by sich fyearq fqtr;
run;
proc sql;
	create table report_delay_6 as
	select a.*, b.rank_max
	from report_delay_4 a, report_delay_5 b
	where a.sich = b.sich and a.fyearq = b.fyearq and a.fqtr = b.fqtr;
quit;
/*	If a firm is the quickest filer then it's ranked in 1st decile, and if slowest I rank it as 10th decile.	*/
/*	However I do not know how to treat middle filers, so I have just left them with the rank assigned by the program.	*/
data report_delay_7; set report_delay_6;
	if delay_rank ne 1 && delay_rank = rank_max then delay_rank = 10;
run;
/*	I do not drop the report delay, because I will use it in the next question.	*/
data &dsout (drop= earliest_report rank_max); set report_delay_7; run;
%mend industryLeaders;
/*	Test if it works.	*/
%industryLeaders(dsin=industry_sales,dsout=industry_rank); 	

/***	Question 3	***/

/*	First part is the preparation of data for regression. Very similar to the code provided in class.	*/
/*	I prepare the reporting delay for industry again, except I calculate the delay rank(decile) by year, instead of quarters.	*/
%macro industryLeaders_y(dsin=,dsout=);
proc sort data=&dsin; by sich fyearq fqtr rdq; run;
data &dsin;set &dsin; where fqtr = 4; run;	/*	I assume that yearly report date is equal to 4th quarter's report date.	*/
proc sql;
	create table first_reporter as
	select sich, fyearq, fqtr, min(rdq) as earliest_report from &dsin
	group by sich, fyearq, fqtr
	having above_median = 1;
quit;
proc sql;
	create table report_delay as
	select a.*, b.earliest_report
	from &dsin a, first_reporter b
	where a.sich = b.sich and a.fyearq = b.fyearq and a.fqtr = b.fqtr;
quit;
data report_delay_2; set report_delay;
	delay = rdq - earliest_report;
	where rdq ne .;	/*	I drop if report date is missing, because I don't know whether it will be the first or last reporting company in that case.	*/
run;
proc rank data = report_delay_2 out=report_delay_3 ties=low groups=10;
	by sich fyearq fqtr;
	var delay;
	ranks delay_rank;
run;
/*	Data is ranked from 0 to 9, I change it from 1 to 10	*/
data report_delay_4; set report_delay_3;
	delay_rank = delay_rank + 1;
run;
proc means data=report_delay_4 noprint; 
	output out= report_delay_5 max(delay_rank)= rank_max;
	var delay_rank;
	by sich fyearq fqtr;
run;
proc sql;
	create table report_delay_6 as
	select a.*, b.rank_max
	from report_delay_4 a, report_delay_5 b
	where a.sich = b.sich and a.fyearq = b.fyearq and a.fqtr = b.fqtr;
quit;
/*	If a firm is the quickest filer then it's ranked in 1st decile, and if slowest I rank it as 10th decile.	*/
/*	However I do not know how to treat middle filers, so I have just left them with the rank assigned by the program.	*/
data report_delay_7; set report_delay_6;
	if delay_rank ne 1 && delay_rank = rank_max then delay_rank = 10;
run;
/*	I do not drop the report delay, because I will use it in the next question.	*/
data &dsout (drop= earliest_report rank_max); set report_delay_7; run;
%mend industryLeaders;
%industryLeaders_y(dsin=industry_sales,dsout=industry_rank_y);

/*	Include the macros.	*/
%include "&myFolder\macro_5_event_return.sas";
%include "&myFolder\macro_winsor.sas";

filename mprint 'c:\temp\sas_macrocode.txt'; options mfile mprint;

/*	We already have the industry_rank_y data as input from the previous part.	We use it as input and bring the relevant information.	*/
/*	I am borrowing part of the getfunda macro provided in the website to acquire other identifiers,	*/
rsubmit;
proc sql; 
  create table c_c_m as 
  select lpermno, gvkey, linkdt, linkenddt, linktype, linkprim
  from crsp.ccmxpf_linktable 
    where lpermno ne . 
    and linktype in ("LC" "LN" "LU" "LX" "LD" "LS") 
    and linkprim IN ("C", "P"); 
quit;
proc download data=c_c_m out=c_c_m;run;
endrsubmit;

proc sql; 
  create table getf_3 as 
  select a.*, b.lpermno as permno
  from industry_rank_y a left join c_c_m b 
    on a.gvkey eq b.gvkey 
    and b.lpermno ne . 
    and b.linktype in ("LC" "LN" "LU" "LX" "LD" "LS") 
    and b.linkprim IN ("C", "P")  
    and ((a.datadate >= b.LINKDT) or b.LINKDT eq .B) and  
       ((a.datadate <= b.LINKENDDT) or b.LINKENDDT eq .E)   ; 
quit; 
proc sort data = getf_3 nodupkey; by gvkey fyear; run;
data getf_3; set getf_3; where permno ne .;run;	/*	Permno for some companies could be backfilled, but for the sake of this exercise, I am dropping the missing observations.	*/
/* retrieve historic cusip */
rsubmit;
proc sql;
	create table dse_names as
	select ncusip, permno, namedt, nameendt
	from crsp.dsenames
	where ncusip ne "";
quit;
proc download data=dse_names out=dse_names; run;
endrsubmit;

proc sql;
  create table getf_4 as
  select a.*, b.ncusip
  from getf_3 a, dse_names b
  where 
        a.permno = b.PERMNO
    and b.namedt <= a.datadate <= b.nameendt
    and b.ncusip ne "";
  quit;
/* force unique records */
proc sort data=getf_4 nodupkey; by gvkey fyearq fqtr;run;

rsubmit;
proc upload data=getf_4 out=getf_4; run;  
proc sql;
  create table industry_rank_id as
  select distinct a.*, b.ticker as ibes_ticker
  from getf_4 a left join ibes.idsum b
  on 
        a.NCUSIP = b.CUSIP
    and a.datadate > b.SDATES ;
quit;
proc sort data=industry_rank_id nodupkey; by gvkey fyear;run;
proc download data=industry_rank_id out=industry_rank_id; run;
endrsubmit;


/*	At this stage we have the industry data, delay ranks, identifiers and the earnings report date.	*/
/*	We now compute beta.	*/

data ue_1 (keep = gvkey fyear datadate ibes_ticker );	set industry_rank_id;	if ibes_ticker ne "";	run;
rsubmit;
proc upload data=ue_1 out=ue_1; run; 
proc upload data= industry_rank_id out= industry_rank_id; run;
/* consensus forecast */
proc sql;	
	create table ue_2 as 
	select a.*, b.meanest, b.statpers
	from ue_1 a left join ibes.statsum_epsus b
	on a.ibes_ticker = b.ticker 
	and missing(b.meanest) ne 1
    and b.measure="EPS"
    and b.fiscalp="ANN"
    and b.fpi = "1"
    and a.datadate - 40 < b.STATPERS < a.datadate 
    and a.datadate -5 <= b.FPEDATS <= a.datadate +5 
	/* take most recent one in case of multiple matches */
	group by ibes_ticker, datadate
	having max(b.statpers) = b.statpers; 
quit;
proc download data = ue_2 out = ue_2; run;
/* get actual earnings */
proc sql;
  create table ue_3 as
  select a.*, b.PENDS, b.VALUE, b.ANNDATS, b.value - a.meanest as unex, abs( calculated unex) as absunex
  from ue_2 a left join ibes.act_epsus b
  on 
        a.ibes_ticker = b.ticker
	and missing(b.VALUE) ne 1
    and b.MEASURE="EPS"
    and b.PDICITY="ANN"
    and a.datadate -5 <= b.PENDS <= a.datadate +5;
quit;
/* force unique records - keep the one with largest surprise*/
proc sort data=ue_3; by gvkey datadate descending absunex;run;
proc sort data=ue_3 nodupkey; by gvkey datadate ;run;
proc download data = ue_3 out = ue_3; run;
proc sql;
	create table industry_unex as 
	select a.*, b.* from industry_rank_id a left join ue_3 b on a.gvkey = b.gvkey and a.datadate = b.datadate;
quit;
proc sort data = industry_unex nodupkey; by gvkey fyear; run;
proc download data = industry_unex out= industry_unex; run;
endrsubmit;
/****/
data industry_unex_key; set industry_unex; key = gvkey || fyear; where unex ne .; run;


%macro getBeta(dsin=, dsout=, nMonths=, minMonths=12, estDate=);
/* create return window dates: mStart - mEnd */
data getb_1 (keep = key permno mStart mEnd);
set &dsin; 
/* drop obs with missing estimation date */
if &estDate ne .;
mStart=INTNX('Month',&estDate, -&nMonths, 'E'); 
mEnd=INTNX('Month',&estDate, -1, 'E'); 
if permno ne .;  
format mStart mEnd date.;
run;
  
/* get stock and market return */
rsubmit;
proc upload data = getb_1 out=getb_1; run;
proc sql;
  create table getb_2
    (keep = key permno mStart mEnd date ret vwretd) as
  select a.*, b.date, b.ret, c.vwretd
  from   getb_1 a, crsp.msf b, crsp.msix c
  where a.mStart <= b.date <= a.mEnd 
  and a.permno = b.permno
  and missing(b.ret) ne 1
  and b.date = c.caldt;
quit;
proc download data = getb_2 out=getb_2; run;
endrsubmit;
/* force unique obs */  
proc sort data = getb_2 nodup;by key date;run;

/* estimate beta for each key 
	EDF adds R-squared (_RSQ_), #degrees freedom (_EDF_) to regression output
*/
proc reg outest=getb_3 data=getb_2;
   id key;
   model  ret = vwretd  / noprint EDF ;
   by key;
run;

/* drop if fewer than &minMonths used*/
%let edf_min = %eval(&minMonths - 2);
%put Minimum number of degrees of freedom: &edf_min;

/* create output dataset */
proc sql;
  create table &dsout as 
	select a.*, b.vwretd as beta 
	from &dsin a left join getb_3 b on a.key=b.key and b._EDF_ > &edf_min;
quit;
%mend;
%getBeta(dsin=industry_unex_key, dsout=industry_beta, nMonths=30, minMonths=12, estDate=datadate);
/****/


%macro eventReturn(dsin=, dsout=, eventdate=, start=0, end=1, varname=);
data er_1 (keep = key permno beta &eventdate);
set &dsin;
if &eventdate ne .;
if beta ne .;
run;
/* measure the window using trading days*/
rsubmit;
proc sql; create table er_2 as select distinct date from crsp.dsf;quit;
proc download data = er_2 out = er_2; run;
endrsubmit;
/* create counter */
data er_2; set er_2; count = _N_;run;
/* window size in trading days */
%let windowSize = %eval(&end - &start); 
/* get the closest event window returns, using trading days */
proc sql;
	create table er_3 as 
	select a.*, b.date, b.count
	from er_1 a, er_2 b	
	/* enforce 'lower' end of window: trading day must be on/after event (not before)
		using +10 to give some slack at the upper end */
	where b.date >= (a.&eventdate + &start) and b.date <=(a.&eventdate + &end + 10)
	group by key
	/* enforce 'upper' end of window: minimum count + windowsize must equal maximum count */
	having min (b.count) + &windowSize <= b.count;
quit;
/* determine the start trading day of return window */
proc sql;
	create table er_3 as 
	select a.*, b.count as wS
	from er_1 a, er_2 b	
	where b.date >= (a.&eventdate + &start)
	group by key
	having min (b.count) = b.count ;
quit;
/* pull in trading days for event window */
proc sql;
	create table er_4 as 
	select a.*, b.date
	from er_3 a, er_2 b	
	where a.ws <= b.count <= a.ws + &windowSize;
quit;
proc sort data=er_4; by key date;run;

/* append firm return and index return */
rsubmit;
proc upload data = er_4 out = er_4; run;
proc sql;
	create table er_5 as
	select a.*, b.ret, c.vwretd, b.ret - a.beta * c.vwretd as abnret
	from er_4 a, crsp.dsf b, crsp.dsix c
	where a.permno = b.permno
	and a.date = b.date
	and b.date = c.caldt
	and missing(b.ret) ne 1; 
quit;
proc download data = er_5 out= er_5; run;
endrsubmit;

proc sql;
	create table er_6 as 
	select key, exp(sum(log(1+abnret)))-1 as abnret
	from er_5 group by key;
quit;

/* create output dataset */
proc sql;
	create table &dsout as
	select a.*, b.abnret as &varname
	from &dsin a left join er_6 b
	on a.key = b.key;
quit;
%mend;

%eventReturn(dsin=industry_beta, dsout=e_ret, eventdate=datadate, start=-1, end=2, varname=abnret);
%eventReturn(dsin=e_ret, dsout=e_ret2, eventdate=datadate, start=0, end=1, varname=abnret2);


/* Variable construction */
rsubmit; 
proc upload data = e_ret2 out= e_ret2; run;
proc sql;
	create table e_ret2_prices as
	select a.*, b.prcc_f, b.ni
	from e_ret2 a, comp.funda b
	where a.gvkey=b.gvkey and a.fyear = b.fyear;
quit;
proc download data= e_ret2_prices out=e_ret2_prices; run;
endrsubmit;

data f_sample;
set e_ret2_prices;
unex_p = unex / prcc_f;
loss = (ni < 0);
loss_unex_p = loss * unex_p;
run;

/* winsorize */
%let myVars = delay_rank loss loss_unex_p unex_p abnret abnret2;
%winsor(dsetin=f_sample,  byvar=fyear, dsetout=f_sample_wins, vars=&myVars , type=winsor, pctl=1 99);

/*	Now that we have the data ready we can construct/use regression macro.	*/

/*	In order to not lose data I have so far, I am saving it. This step is redundant if code is run as a whole.	*/

libname repl "C:\Users\gunervelioglu\Desktop\ACG 6935\week 5";
data repl.f_sample_wins; set f_sample_wins;
run;
/*	Save & Load	*/
libname repl "C:\Users\gunervelioglu\Desktop\ACG 6935\week 5";
data f_sample_wins; set repl.f_sample_wins;
run;


/*	Macro for regression.	*/
%macro doReg(method=, results=);

%if &method eq surveyreg %then %do;
proc surveyreg data=f_sample_wins;   
   	model abnret = delay_rank unex_p loss loss_unex_p;  
	ods output 	ParameterEstimates  = &results._params
	            FitStatistics 		= &results._fit
				DataSummary 		= &results._summ;
quit;
%end;
%if &method eq reg %then %do;
proc reg data=f_sample_wins;   
   model abnret = delay_rank unex_p loss loss_unex_p;
   ods output 	ParameterEstimates  = &results._params
	            FitStatistics 		= &results._fit
				DataSummary 		= &results._summ;

quit;
%end;
%mend;
%doReg(method=surveyreg, results=work.reg1);
%doReg(method=reg, results=reg2);
