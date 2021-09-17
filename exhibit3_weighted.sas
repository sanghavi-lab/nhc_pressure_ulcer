*THIS CODE IS TO CALCULATE WEIGHTED 25TH AND 75TH PERCENTILE OF NURSING HOME REPORTING RATE FOR EXHIBIT 3;
*https://blogs.sas.com/content/iml/2016/08/29/weighted-percentiles.html;

libname out '\\prfs.cri.uchicago.edu\sanghavi-lab\Zoey\gardner\data\exhibits\pu\';

proc import datafile="\\prfs.cri.uchicago.edu\sanghavi-lab\Zoey\gardner\data\merge_output\pu\medpar_mds_final\analysis\report rate\report_rate_nh_table_weight2.csv"
		out=main
		dbms=csv
		replace;
		guessingrows=7000;
run;

ods csvall file='\\prfs.cri.uchicago.edu\sanghavi-lab\Zoey\gardner\data\exhibits\pu\exhibit3_quantile_weight_by_claims.csv';

proc means data=main p25 p75;
	class claims_type highest_stage short_stay;
	var report_at_all__all_claims_ report_at_correct_stage_____1___;
	*where short_stay="True" & highest_stage=4 & claims_type="MedPAR Hospital Claims with Primary Diagnosis as Pressure Ulcer";
	weight weight_claims;
run;
ods csvall close;


