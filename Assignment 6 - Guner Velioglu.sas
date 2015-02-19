/*	Download the macros.	*/
filename m1 url 'http://www.wrds.us/macros/array_functions.sas';
%include m1;
filename m2 url 'http://www.wrds.us/macros/runquit.sas';
%include m2;

/*	Guner Velioglu	--	61441902	*/

/***	ASSIGNMENT 6	***/

/* Login to WRDS. */
rsubmit;endrsubmit;
%let wrds = wrds.wharton.upenn.edu 4016;options comamid = TCP remote=WRDS;
signon username=_prompt_;

/***	Question 1	***/
libname edgar "C:\Users\gunervelioglu\Desktop\ACG 6935\edgar";

rsubmit;
proc sql;
	create table a_funda as
	select gvkey, fyear, datadate, cik
	from comp.funda 
		where indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C';
quit;
proc download data = a_funda out = a_funda; run;
endrsubmit;

/*	We have a_funda as starting list, and our goal is to bring number of 8-K s for each year.	*/
/*	Since CIK is changing, we need the historical CIK s provided by secsuite.	*/

%macro bring8k(dsin=, dsout=);
rsubmit;
libname secsuite "/wrds/sec/sasdata";
proc download data=secsuite.wciklink_gvkey out=wciklink_gvkey; run; 
endrsubmit;

data a_filings; set edgar.filings; run;
data f_filings; set a_filings; 
	date_year = year(date);
	where formtype = "8-K";
	filed_8k = 1; 
	run;
proc sort data = f_filings; by cik date_year; run;

proc means data = f_filings noprint; output out = f_filings_count n= /autoname;
	var filed_8k;
	by cik date_year;
run;

data a_funda_2; set &dsin; 
	cik_num = cik*1;
	datadate_year = year(datadate);
	if fyear = . then fyear = datadate_year;	/*	If firm year is missing, I use the year of datadate.	*/
	if fyear = . and datadate_year = . then delete;
run;

proc sql;
	create table a_funda_8k as
	select a.*, b.date_year, b.filed_8k_N
	from a_funda_2 a left join f_filings_count b
	on a.cik_num = b.cik and a.fyear = b.date_year;
quit;

data a_funda_8k_2 (keep=gvkey fyear datadate cik_num filed_8k_N); set a_funda_8k; run;

/*	Now for the missing cases, I'll see if we can bring information by using the historical CIK.	*/
proc sql;
	create table a_funda_sec as
	select a.*, b.cik
	from a_funda_8k_2 a left join wciklink_gvkey b
	on a.gvkey = b.gvkey 
	and b.datadate1 <= a.datadate <= b.datadate2;
quit;

data a_funda_sec_2 (drop= cik); set a_funda_sec;
	cik_num_2 = cik*1;
	if cik_num = . and cik_num_2 = . then delete;	/*	If both of them are missing then we can not bring 8-K information.	*/
run;

proc sql;
	create table a_funda_8k_matched as
	select a.*, b.date_year, b.filed_8k_N as filed_8k_N_2
	from a_funda_sec_2 a left join f_filings_count b
	on a.cik_num_2 = b.cik and a.fyear = b.date_year;
quit;

data a_funda_8k_matched_2 (keep = gvkey fyear filed_8k_N); set a_funda_8k_matched;
	if filed_8k_N = . then filed_8k_N = filed_8k_N_2;
run;
proc sort data = a_funda_8k_matched_2 nodupkey; by gvkey fyear; run;
data &dsout; set a_funda_8k_matched_2; run;
%mend;
/*	Test if it works.	*/
%bring8k(dsin= a_funda, dsout= a_funda_8k_count);

/***	Question 2	***/

/*	Create the dataset.	*/
data absorb_test;
  input @01 depv      
  			@03 indv1
       		@05 indv2
			@07 indv3
			@09 groupid;
datalines;
1 1 2 9 1
2 3 2 7 1
3 4 1 4 1
4 5 3 3 1
5 6 2 3 1
6 7 3 2 1
7 8 7 1 1
8 9 2 4 1
1 2 7 8 1
2 3 3 7 2
4 4 7 5 2
1 2 2 9 2
5 6 2 6 2
6 8 3 3 2
3 5 5 7 2
8 9 2 2 2
2 4 9 8 2
run;

proc standard data=absorb_test mean=0 out=absorb_demeaned;
  var depv indv1 indv2 indv3;
  by groupid;
run;

proc reg data=absorb_demeaned outest= absorb_demeaned_reg;   
   model depv = indv1 indv2 indv3;
quit;

proc sort data = absorb_test; by groupid; run;
proc glm data = absorb_test; 
	absorb groupid;
	model depv = indv1 indv2 indv3;
run;
quit;

/*	They match!	*/

/***	Question 3	*/
/*	Download FundA	*/
rsubmit;
proc sql;
	create table m_funda as
	select conm, gvkey, fyear, datadate, at, wcap, ni, lt, txditc, prcc_f, csho, pstkl
	from comp.funda 
		where indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C';
quit;
proc download data = m_funda out = m_funda; run;
endrsubmit;

data m_funda_2; set m_funda; run;
proc sort data = m_funda_2 nodupkey; by gvkey fyear; run;

proc sql;
	create table m_funda_3 as
	select conm, gvkey, fyear, datadate, at, wcap, ni, lt, txditc, prcc_f, csho, pstkl, min(fyear) as first_year, 2014 - min(fyear) as comp_years from m_funda_2
	group by gvkey;
quit;
/*	Calculate the control variables.	*/
data m_funda_vars; set m_funda_3;
	if ni ne . and ni < 0 then loss = 1;
	if ni ne . and ni >= 0 then loss = 0;
	wcap_to_assets = wcap / at;
	market_to_book = (lt - txditc + prcc_f * csho + pstkl) / at;	/*	I used the market-to-book ratio as defined here (pg. 47): http://papers.ssrn.com/sol3/papers.cfm?abstract_id=2071336	*/
	book_leverage = lt / at; 
run;
/*	Find predicted loss probabilities.	*/
proc logistic data = m_funda_vars; output out = m_funda_vars_pred PREDICTED=predicted_loss;
	model loss = wcap_to_assets market_to_book comp_years;
run;

data m_funda_vars_2; set m_funda_vars_pred;
	where predicted_loss ne .;
run;
/*	Determine the groups according to name and last name.	*/
data m_funda_names (drop = last_name); set m_funda_vars_2;
	first_letter_first = substr(conm, 1 , 1);
	last_name = substr(conm, index(conm, ' ') + 1, 1);
	first_letter_last = substr(last_name, 1 , 1);
		if first_letter_first <=: 'L' then first_group = 'A-L';
  			else first_group = 'M-Z';
		if first_letter_last <=: 'L' then last_group = 'A-L';
  			else last_group = 'M-Z';
		if first_group = 'A-L' then group_unmatched = 1;
		if last_group = 'M-Z' then group_unmatched = 2;
run;

data sample_a_l; set m_funda_names;
	where first_group = 'A-L';
run;
proc sort data = sample_a_l; by gvkey fyear; run;
data sample_m_z; set m_funda_names;
	where last_group = 'M-Z';
run;
/*	Left join the control firms.	*/
proc sql;
	create table sample_matched as
	select a.conm, a.gvkey, a.fyear, a.loss, a.wcap_to_assets, a.market_to_book, a.comp_years, a.predicted_loss,
			b.conm as conm2, b.wcap_to_assets as wcap_to_assets2, b.market_to_book as market_to_book2, b.comp_years as comp_years2, b.predicted_loss as predicted_loss2
	from sample_a_l a left join sample_m_z b
	on a.fyear = b.fyear
	and b.predicted_loss - 0.01 < a.predicted_loss < b.predicted_loss + 0.01;
quit;

data sample_matched_2; set sample_matched;
	loss_diff = abs(predicted_loss - predicted_loss2);
run;
/*	In order to keep the best match, I only keep the cases where distance is minimum. This part takes around 4 minutes.	*/
/*	I tried to do everything at once with sql, but strangely, it takes even more time.	*/
proc sort data = sample_matched_2 nodupkey; by gvkey fyear loss_diff; run;
proc sort data = sample_matched_2 nodupkey; by gvkey fyear; run;

libname repl "C:\Users\gunervelioglu\Desktop\ACG 6935\week 6";
data repl.sample_matched_2; set sample_matched_2;
run;
/*	Save & Load, so I can start from this step.	*/
libname repl "C:\Users\gunervelioglu\Desktop\ACG 6935\week 6";
data sample_matched_2; set repl.sample_matched_2;
run;

/*	Prepare the matched sample for ttest.	*/
data sample1 (keep = gvkey fyear wcap_to_assets market_to_book comp_years predicted_loss group); set sample_matched_2;
	group = 1;
run;
data sample2 (keep = gvkey fyear wcap_to_assets2 market_to_book2 comp_years2 predicted_loss2); set sample_matched_2; run;
data sample3 (keep = gvkey fyear wcap_to_assets market_to_book comp_years predicted_loss group); set sample2;
	wcap_to_assets = wcap_to_assets2;
	market_to_book = market_to_book2;
	comp_years = comp_years2;
	predicted_loss = predicted_loss2;
	group = 2;
run;

proc append base=sample1 data=sample3; run;
data matched_sample_ttest; set sample1; run;

/*	Include the necessary macros, downloaded from github class notes.	*/
%include "C:\Users\gunervelioglu\Desktop\ACG 6935\Sasmacros\commonmacro.sas";
%include "C:\Users\gunervelioglu\Desktop\ACG 6935\Sasmacros\tablebygroup.sas";
%let exportDir=C:\Users\gunervelioglu\Desktop\ACG 6935\Sasmacros\week6;

%tablebygroup(dsin=matched_sample_ttest, vars=wcap_to_assets market_to_book comp_years predicted_loss group, byvar=group, export=matched_tt);
%tablebygroup(dsin=m_funda_names, vars=wcap_to_assets market_to_book comp_years predicted_loss group_unmatched, byvar=group_unmatched, export=unmatched_tt);


/***	Question 4	***/

/*	Note: I wasn't able to complete this question.	*/

/*	I decrease the sample size for easier handling of data.	*/
data sample_1_optional; set m_funda_names; 
	where first_letter_first = "G";
run;
data sample_2_optional; set m_funda_names; 
	where first_letter_last = "V";
run;
/*	Latter sample is the smaller one. 1599 firm-years.	So in final sample we should end up with 1599 firm-years.	*/

proc sql;
	create table sample_matched_optional as
	select a.conm, a.gvkey, a.fyear, a.loss, a.wcap_to_assets, a.market_to_book, a.comp_years, a.predicted_loss,
			b.conm as conm2, b.wcap_to_assets as wcap_to_assets2, b.market_to_book as market_to_book2, b.comp_years as comp_years2, b.predicted_loss as predicted_loss2
	from sample_1_optional a left join sample_2_optional b
	on a.fyear = b.fyear
	and b.predicted_loss - 1 < a.predicted_loss < b.predicted_loss + 1;	/*	Bring every possible combination.	*/
quit;

data sample_matched_opt_diff; set sample_matched_optional;
	loss_diff = abs(predicted_loss - predicted_loss2);
	if loss_diff = . then delete;
run;
proc sort data = sample_matched_opt_diff; by fyear loss_diff; run;

data sample_test; set sample_matched_opt_diff; run;
proc sort data = sample_test nodupkey; by fyear conm2; run;
proc sort data = sample_test nodupkey; by fyear conm; run;

/*	I have 1403 unique matches, less than 1599.	*/
/*	Next iteration should match the unmatched 196.	*/
data sample_test; set sample_test; match_1 = 1; run;
proc sql;
	create table sample_test_2 as
	select a.*, b.match_1
	from sample_matched_opt_diff a left join sample_test b
	on a.gvkey = b.gvkey and a.fyear = b.fyear;
quit;

data sample_test_3; set sample_test_2; where match_1 ne 1; run;
proc sort data = sample_test_3 nodupkey; by fyear conm2; run;
proc sort data = sample_test_3 nodupkey; by fyear conm; run;
/*	54 matches, 142 possible matches left.	*/
/*	This process should be repeated until the last generated matched sample has 0 observations.	*/
