/*	Download the macros.	*/
filename m1 url 'http://www.wrds.us/macros/array_functions.sas';
%include m1;
filename m2 url 'http://www.wrds.us/macros/runquit.sas';
%include m2;

/*	Guner Velioglu	--	61441902	*/

/***	ASSIGNMENT 4	***/

/***	Question 1	***/

/*	Let's first create a sample dataset to better see if this works.	*/
data Firm_Vars;
  input @01 firm_id       
  			@03 var1
       		@05 var2
			@07 var3;
datalines;
1 1 2 9
2 9 2 1
3 3 1 4
4 3 3 3
5 1 2 3
6 2 3 3
7 2 7 1
8 5 2 4
9 6 7 3
run;

/*	I am assuming that instead of 3 variables we may want to check many variables, for example: var1, var2, var3,..., var99, var100.
	We know that these variables go from 1 to 100 (or more) but of course we don't want to enter all variable names manually.	*/

/*	Define the macro which finds the maximum of 'specified' variables.	*/
%macro selectMax(dsin=, vars=, maxvar=);
data &dsin; set &dsin;
&maxvar = max(%do_over(values=&vars, between=comma));
run;
%mend;
/*	Let's see if it works. We should specify the variables by do_over, and the macro will find the maximum one for each observation.	*/
%selectMax(dsin=firm_vars, vars=%do_over(values=1-3, phrase=var?), maxvar=mymax);

/***	Question 2	***/
/* Log in and download data from WRDS. */
rsubmit;endrsubmit;
%let wrds = wrds.wharton.upenn.edu 4016;options comamid = TCP remote=WRDS;
signon username=_prompt_;

rsubmit;
/*	Firstly, download the list of firms with their sich.	*/
proc sql;
	create table a_funda as
		select *
	  	from comp.funda 
  	where 		
		2012 <= fyear <= 2013	/*	I have these filters just to have a smaller data to deal with, although data is still equally complicated for assignment purpose.	*/
	and 6000 <= sich <= 6499
	and indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C';
quit;
/*	Drop doubles.	*/
proc sort data=a_funda nodupkey; by gvkey fyear;run;
proc download data=a_funda out=a_funda;run;
endrsubmit;

/*	Following is the firmyear list we want to append non-missing count to. This can be any list, I select all firms from the FundA file we have.	*/
data test_miss (keep= gvkey fyear); set a_funda; run;	

/*	Define the macro. It requires the list of firms and the funda file. I have the FundA ready before the macro but it can be downloaded inside the macro as well, via rsubmit.	*/
%macro nonMiss(dsin=, dsout=);
rsubmit;
/* Retrieve variables in Funda */
	ods listing close; 
	ods output variables  = varsFunda; 
proc datasets lib = comp; contents data=funda; quit;run; 
	ods output close; 
	ods listing; 
/* keep relevant variables (excluding firm name, gvkey, fyear, etc)*/
data varsFunda ;
	set  varsFunda;
	if 37 <= Num <= 937;
run;
proc download data=varsFunda out=varsFunda;run;
endrsubmit;
/*	Define the array of variables.	*/
%array(variables, data=varsfunda, var=variable);

data non_missing_funda; set a_funda;
missing = 0;
%do_over(variables, phrase = if missing(?) then missing = missing + 1;);
non_missing = &variablesN - missing;
run;
proc sort data = non_missing_funda; by gvkey fyear; run;

proc sql; 
	create table &dsout as
	select a.*, b.non_missing
	from &dsin a left join non_missing_funda b
	on a.gvkey = b.gvkey
	and a.fyear = b.fyear;
quit;
%mend;
%nonMiss(dsin=test_miss, dsout=test_miss_out);

/*	Question 3	*/

/*	On the first part of the code I am just constructing the firm data to pick a firm.	*/
/*	This part is mostly based on the code we have seen in the class.	*/
rsubmit;
data a_ibes_funda (keep = key gvkey fyear datadate conm ajex ajp);
set comp.funda;	
if 2000 <= fyear <= 2013;	
key = gvkey || fyear;	/* create key to uniquely identify firm-year */
if indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C' ;	/* general filter to drop doubles from Compustat Funda */
run;
proc sort data=a_ibes_funda nodupkey; by gvkey fyear;run;
proc download data=a_ibes_funda out=a_ibes_funda;run;
endrsubmit;
/* get permno */
rsubmit;
proc sql;
  create table b_ibes_permno as
  select a.*, b.lpermno as permno
  from a_ibes_funda a left join crsp.ccmxpf_linktable b
    on a.gvkey = b.gvkey
    and b.lpermno ne .
    and b.linktype in ("LC" "LN" "LU" "LX" "LD" "LS")
    and b.linkprim IN ("C", "P") 
    and ((a.datadate >= b.LINKDT) or b.LINKDT = .B) and 
       ((a.datadate <= b.LINKENDDT) or b.LINKENDDT = .E)   ;
quit;
proc download data=b_ibes_permno out=b_ibes_permno;run;
endrsubmit;
rsubmit;
proc sql;
	create table c_ibes_adj as
	select a.*, b.cfacpr, b.cfacshr
	from b_ibes_permno a left join crspa.dsf b
		on a.permno = b.permno
		and b.permno ne .
		and a.datadate = b.date;
quit;
proc download data = c_ibes_adj out = c_ibes_adj; run;
endrsubmit;

/* retrieve historic cusip */
rsubmit;
proc sql;
  create table c_ibes_cusip as
  select a.*, b.ncusip
  from c_ibes_adj a, crsp.dsenames b
  where 
        a.permno = b.PERMNO
    and b.namedt <= a.datadate <= b.nameendt
    and b.ncusip ne "";
  quit;
/* force unique records */
proc sort data=c_ibes_cusip nodupkey; by key;run;
proc download data=c_ibes_cusip out=c_ibes_cusip;run;
endrsubmit;
/* get ibes ticker */
rsubmit;
proc sql;
  create table d_ibestick as
  select distinct a.*, b.ticker as ibes_ticker
  from c_ibes_cusip a, ibes.idsum b
  where 
        a.NCUSIP = b.CUSIP
    and a.datadate > b.SDATES ;
quit;
proc download data=d_ibestick out=d_ibestick;run;
endrsubmit;
rsubmit;
proc sql;
  create table d_ibes_adj_splitdate as
  select distinct a.*, b.adj, b.spdates
  from d_ibestick a, ibes.adj b
  where 
        a.NCUSIP = b.CUSIP
    and a.datadate >= b.SPDATES >= a.datadate-365;
quit;
proc download data=d_ibes_adj_splitdate out=d_ibes_adj_splitdate;run;
endrsubmit;
/* get number of estimates -- last month of fiscal year*/
rsubmit;
proc sql;
  create table e_numanalysts as
  select a.*, b.STATPERS, b.numest as num_analysts
  from d_ibes_adj_splitdate a, ibes.STATSUMU_EPSUS b
  where 
        a.ibes_ticker = b.ticker
    and b.MEASURE="EPS"
    and b.FISCALP="ANN"
    and b.FPI = "1"
    and a.datadate - 30 < b.STATPERS < a.datadate 
    and a.datadate -5 <= b.FPEDATS <= a.datadate +5;
quit;
/* force unique records */
proc sort data=e_numanalysts nodupkey; by key;run;
proc download data=e_numanalysts out=e_numanalysts;run;
endrsubmit;
/* append num_analysts to b_permno */
rsubmit;
proc sql;
    create table f_funda_analysts as 
    select a.*, b.num_analysts 
    from d_ibes_adj_splitdate a 
    left join e_numanalysts b 
    on a.key=b.key;
quit;
/* missing num_analysts means no analysts following */
data f_funda_analysts;
set f_funda_analysts;
if permno ne . and num_analysts eq . then num_analysts = 0;
run;
proc download data=f_funda_analysts out=f_funda_analysts;run;
endrsubmit;
proc sort data = f_funda_analysts; by conm; run;
data g_funda_analysts ; set f_funda_analysts;
where num_analysts > 0;
run;

/*	I now decide on a firm and prepare the sample input file for the macro.	*/
/*	Looking at this list(g_funda_analysts), I decide on 1ST CONSTITUTION BANCORP, which has several stock splits.	*/ 
/*	Purpose of the macro will be to bring the adjusted forecasts for each stock split.	*/
data semicon_splits (keep= ncusip ibes_ticker adj spdates); set g_funda_analysts;
where ibes_ticker = "FCCY";
run;


%macro adj(dsin=, dsout=);
rsubmit;
/*	Download the firm related information from the adjusted forecasts.	*/
proc sql;
  create table semicon_det as
  select *
  from ibes.DET_EPSUS b
  where ticker eq "FCCY";
quit;
proc download data=semicon_det out=semicon_det;run;
endrsubmit;
rsubmit;
/*	Download the firm related information from the unadjusted forecasts.	*/
proc sql;
  create table semicon_detu as
  select *
  from ibes.DETU_EPSUS b
  where ticker eq "FCCY";
quit;
proc download data=semicon_detu out=semicon_detu;run;
endrsubmit;
/*	Keep the forecasts whic are made after the stock split date.	*/
proc sql;
	create table semicon_details as
	select a.*, b.actdats as actdats_det, b.analys as analys_det, b.value as value_det
	from &dsin a left join semicon_det b
	on a.ncusip = b.cusip and a.spdates <= b.actdats;
quit;
proc sort data=semicon_details; by spdates actdats_det; run;
/*	Keep only the closest forecast to the stock split date.	*/
proc sql;
	create table semicon_details2 as
	select * from semicon_details
	group by spdates
	having (actdats_det-spdates) = min(actdats_det-spdates);
quit;
/*	Same procedure for unadjusted part.	*/
proc sql;
	create table semicon_details3 as
	select a.*, b.actdats as actdats_detu, b.analys as analys_detu, b.value as value_detu
	from semicon_details2 a left join semicon_detu b
	on a.ncusip = b.cusip and a.spdates <= b.actdats;
quit;
proc sort data=semicon_details3; by spdates actdats_detu; run;
proc sql;
	create table semicon_details4 as
	select * from semicon_details3
	group by spdates
	having (actdats_detu-spdates) = min(actdats_detu-spdates);
quit;
/*	Filter the observations where adjusted and unadjusted values match after correction by adjustment factor.	*/
data semicon_details5; set semicon_details4;
	value_estimated = value_detu/adj;;
run;
data semicon_details6; set semicon_details5;
	where value_estimated >= value_det >= value_estimated - 0.001 | value_det = value_detu;
run;
/*	Calculate the adjusted forecast.	*/
data &dsout (keep = ncusip ibes_ticker spdates actdats_det analys_det value_detu value_det adj adjusted_value); set semicon_details6;
	adjusted_value = value_detu/adj;
	if value_detu = value_det then adjusted_value = value_det;
run;
%mend;

/*	Let's see if it works.	*/
%adj(dsin=semicon_splits, dsout=adjusted_test);

