/*	Guner Velioglu	--	61441902	*/

/***	ASSIGNMENT 1	***/

/* Log in and download Compustat - Funda data from WRDS. */

%let wrds = wrds.wharton.upenn.edu 4016;options comamid = TCP remote=WRDS;
signon username=_prompt_;

rsubmit;

data sales (keep = gvkey fyear datadate sich sale);
set comp.funda;
if indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C' ;
proc download data=sales out=sales;
run;

endrsubmit;

/***	First Question	***/

/*	I first find how many observations exist for each firm-year.	*/
proc sort data=sales ; by gvkey fyear;run;
proc means data=sales NOPRINT;
  OUTPUT OUT=sales1Output n= /autoname;
  by gvkey fyear;
run;

/*	Then I list the cases where multiple observations exist for each firm-year.	*/
data duplicate_firm_years;
set sales1output;
where _freq_ > 1;
if fyear = . then delete;	/*	I drop duplicates in terms of missing years since we are not interested in them.	*/
keep gvkey fyear _freq_;
run;

/***	Second Question	***/

/*	I drop the cases where sale or industry code variable is missing.	*/
data sales2;
set sales;
if sale = . or sich = . then delete;
run;

proc sort data=sales2 nodupkey; by gvkey fyear;run;
proc sort data=sales2; by sich gvkey;run;

/*	Lifetime sales for each firm.	*/
proc means data=sales2 NOPRINT;
  OUTPUT OUT=firm_sales sum= /autoname;
  var sale;
  by sich gvkey;
run;
/*	Lifetime sales for each industry.	*/
proc means data=firm_sales NOPRINT;
  OUTPUT OUT=sales_industry_output sum= /autoname;
  var sale_Sum;
  by sich;
run;
data industry_sales; set sales_industry_output; where _freq_ > 19; run;
data industry_sales (keep= sich sale_sum_sum); set industry_sales; run;

/***	Third Question	***/

/*	I first drop the missing observation cases and generate an industry-year variable.	*/
data sales3;
set sales;
if sale = . or sich = . then delete;
ind_year = sich || "_" || fyear;
run;

proc sort data = sales3; by ind_year; run; 

/*	Following code is based on the code we learned in lab session.	*/
/*	I will find the industry level sales for each year and then merge it to the firm sales.	*/

data ind_year_sales;
set sales3;
retain Ind_Sales;
by ind_year;
if first.ind_year then ind_sales = 0;
if sale ne .;
if sich ne .;
ind_sales = ind_sales + sale;
if last.ind_year then output;
run;

proc sql;
create table firm_ind_sales as
	select a.*, b.ind_sales
	from sales3 a, ind_year_sales b
	where a.ind_year = b.ind_year;
run;

/*	Using the firm-year sales and industry-year sales I calculate the Herfindahl Index.	*/
proc sort data=firm_ind_sales; by ind_year; run;
data herfindahl (keep= Herfindahl_Index fyear sich); set firm_ind_sales;
if ind_sales = 0 then ind_sales = ind_sales + 1;
h1 = sale/ind_sales;
h2 = h1**2;
retain Herfindahl_Index;
by ind_year;
if first.ind_year then Herfindahl_Index = 0;
Herfindahl_Index = Herfindahl_Index + h2;
if last.ind_year then output;
run;

proc sort data = herfindahl; by sich fyear; run;
