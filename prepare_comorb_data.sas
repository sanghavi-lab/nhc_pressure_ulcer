*transpose data to calculate comorbidity score;
dm 'log;clear;output;clear;';
*read in data;
proc import datafile="\\prfs.cri.uchicago.edu\sanghavi-lab\Zoey\gardner\data\merge_output\pu\medpar_mds_final\secondary_only_merge_samenh_sl.csv"
	out=samenh
	dbms=csv
	replace;
	guessingrows=1000;
run;

proc sort data=samenh;
by MEDPAR_ID;
run;

proc print data=samenh(obs=5);
run;

*transpose diagnosis data from wide to long;
proc transpose 
	data=samenh(keep=MEDPAR_ID
					 DGNS_1_CD--DGNS_25_CD) 
	out=samenh_l1;
by MEDPAR_ID;
var DGNS_1_CD--DGNS_25_CD;
run;

proc print data=samenh_l1(obs=51);
run;

*transpose diagnosis version data from wide to long;
proc transpose 
	data=samenh(keep=MEDPAR_ID
					 DGNS_VRSN_CD_1-DGNS_VRSN_CD_25) 
	out=samenh_l2;
by MEDPAR_ID;
var DGNS_VRSN_CD_1-DGNS_VRSN_CD_25;
run;

proc print data=samenh_l2(obs=26);
run;

*create a numeric column to merge icd and icd_version code;
data samenh_l1;
set samenh_l1;
n=compress(_name_, '_', 'A');
run;

data samenh_l2;
set samenh_l2;
n=compress(_name_, '_', 'A');
run;

*merge diagnosis and diagnosis version long data;
proc sql;
create table samenh_long as
select l1.MEDPAR_ID, l1.n,
	   l1._name_ as icd,
	   l1.col1 as DX,
	   l2._name_ as icd_ver,
	   l2.col1 as icd_ver_code
	from samenh_l1 as l1,
		 samenh_l2 as l2
where l1.MEDPAR_ID=l2.MEDPAR_ID and
	  l1.n=l2.n;
quit;


*change data type of icd version code to "09" and "10";
data samenh_long(drop=icd_ver_code);
set samenh_long;
icd_ver_code2 = put(icd_ver_code, z2.);
Dx_CodeType = tranwrd(icd_ver_code2, '00', '10');
run;

proc sort data=samenh_long;
by medpar_id;
run;

data samenh_long;
set samenh_long;
rename medpar_id=patid;
run;

proc print data=samenh_long(obs=27);
run;

proc sort data=samenh_long
out=samenh_long_nodup
nodupkey;
by patid DX Dx_CodeType;
run;


    
