local indir "//prfs.cri.uchicago.edu/sanghavi-lab/Zoey/gardner/data/merge_output/pu/medpar_mds_final/"

*log using `"`logdir'/Exhibit4.log"', text replace

cd "`indir'"

*load analysis dataset
import delimited using main_model_data, clear

*set up factor variables
*race
encode race_name, gen(race_n)

*region
encode region, gen(region_n)

*size
egen size_n=group(size)
label define size_n 1 "large" 2 "medium" 3 "small"
label values size_n size_n

*ownership
encode ownership, gen(ownership_n)

*age
encode age_bin, gen(age_n)

*dual
egen dual_n=group(dual)
label define dual_n 1 "nondual" 2 "dual"
label values dual_n dual_n

*female
egen female_n=group(female)
label define female_n 1 "male" 2 "female"
label values female_n female_n

*short_stay
egen short_stay_n=group(short_stay)
label define short_stay_n 1 "long-stay" 2 "short-stay"
label values short_stay_n short_stay_n

*disability
egen disability_n=group(disability)
label define disability_n 1 "no-disability" 2 "disability"
label values disability_n disability_n


*highest stage
egen higheststage_n=group(higheststage)
label define higheststage_n 0 "missing" 1 "stage1" 2 "stage2" 3 "stage3" 4 "stage4" 5 "unstageable" 6 "dpi"
label values higheststage_n higheststage_n

*set panel variable
encode mcare_id, gen(prvdrnum)
xtset prvdrnum



*create varlists
global race_dev "americanindian_dev asian_dev black_dev hispanic_dev other_dev"
global race_percentage "american_indian_percent asian_percent black_percent hispanic_percent other_percent"
global higheststage_dev ///
	"hpu_stage_w_20_dev hpu_stage_w_30_dev hpu_stage_w_40_dev hpu_stage_w_50_dev"
global higheststage_percentage ///
	"hpu_stage_w_20_percentage_wo hpu_stage_w_30_percentage_wo hpu_stage_w_40_percentage_wo hpu_stage_w_50_percentage_wo"
global level1_interaction ///
	c.americanindian_dev#c.hpu_stage_w_20_dev  c.americanindian_dev#c.hpu_stage_w_30_dev  c.americanindian_dev#c.hpu_stage_w_40_dev  c.americanindian_dev#c.hpu_stage_w_50_dev ///
    c.asian_dev#c.hpu_stage_w_20_dev           c.asian_dev#c.hpu_stage_w_30_dev 		    c.asian_dev#c.hpu_stage_w_40_dev  		   c.asian_dev#c.hpu_stage_w_50_dev ///
    c.black_dev#c.hpu_stage_w_20_dev 		     c.black_dev#c.hpu_stage_w_30_dev 		    c.black_dev#c.hpu_stage_w_40_dev  		   c.black_dev#c.hpu_stage_w_50_dev ///
    c.hispanic_dev#c.hpu_stage_w_20_dev 	     c.hispanic_dev#c.hpu_stage_w_30_dev 	 	c.hispanic_dev#c.hpu_stage_w_40_dev 	   c.hispanic_dev#c.hpu_stage_w_50_dev ///
    c.other_dev#c.hpu_stage_w_20_dev 		     c.other_dev#c.hpu_stage_w_30_dev 		    c.other_dev#c.hpu_stage_w_40_dev 		   c.other_dev#c.hpu_stage_w_50_dev 

global level2_interaction ///
	c.american_indian_percent#c.hpu_stage_w_20_percentage_wo  c.american_indian_percent#c.hpu_stage_w_30_percentage_wo  c.american_indian_percent#c.hpu_stage_w_40_percentage_wo  c.american_indian_percent#c.hpu_stage_w_50_percentage_wo ///
    c.asian_percent#c.hpu_stage_w_20_percentage_wo           c.asian_percent#c.hpu_stage_w_30_percentage_wo 		    c.asian_percent#c.hpu_stage_w_40_percentage_wo  		    c.asian_percent#c.hpu_stage_w_50_percentage_wo ///
    c.black_percent#c.hpu_stage_w_20_percentage_wo 		    c.black_percent#c.hpu_stage_w_30_percentage_wo 		    c.black_percent#c.hpu_stage_w_40_percentage_wo  		    c.black_percent#c.hpu_stage_w_50_percentage_wo ///
    c.hispanic_percent#c.hpu_stage_w_20_percentage_wo 	    c.hispanic_percent#c.hpu_stage_w_30_percentage_wo 	 	c.hispanic_percent#c.hpu_stage_w_40_percentage_wo 	    c.hispanic_percent#c.hpu_stage_w_50_percentage_wo ///
    c.other_percent#c.hpu_stage_w_20_percentage_wo 		    c.other_percent#c.hpu_stage_w_30_percentage_wo 		    c.other_percent#c.hpu_stage_w_40_percentage_wo 		    c.other_percent#c.hpu_stage_w_50_percentage_wo 
	
global other ///
	  i.female_n ib(last).age_n i.disability_n dual_dev combinedscore i.medpar_yr_num ///
	  ami_final alzh_final alzh_demen_final atrial_fib_final cataract_final chronickidney_final ///
	  copd_final chf_final diabetes_final glaucoma_final hip_fracture_final ischemicheart_final ///
	  depression_final osteoporosis_final ra_oa_final stroke_tia_final cancer_breast_final ///
	  cancer_colorectal_final cancer_prostate_final cancer_lung_final cancer_endometrial_final ///
	  anemia_final asthma_final hyperl_final hyperp_final hypert_final hypoth_final ///	
	  dual_percent ib(first).ownership_n ib(first).region_n ib(first).size_n

*-------*-------*-------*-------*-------*-------*-------*-------*-------*-------*-------*-------*-------;
*short-stay
*run CMC full model	
eststo shortstay_logit: melogit m_pu $race_dev $race_percentage $higheststage_dev $higheststage_percentage ///
						$level1_interaction $level2_interaction $other ///
						if short_stay_n==2 || prvdrnum:
					
esttab shortstay_logit using main_shortstaylogit_regression.csv, se nogap label	replace	

// estimates restore shortstay_logit
// testparm $level1_interaction
// testparm $level1_interaction $level2_interaction
//				
			
// estimates restore shortstay_logit			
// eststo mblack_dev: margins, dydx(black_dev)	post	
//
// estimates restore shortstay_logit
// eststo mhpu_4_dev: margins, dydx(hpu_stage_w_40_percentage_wo) post

// estimates restore shortstay_logit
// eststo mslogit_race: margins, over(race_n) post
// esttab mslogit_race, label ci nogap

* CALCULATE PREDICTIVE REPORTING RATES AT EACH STAGE FOR SHORT-STAY RESIDENTS
// margins, at((means) _all black_dev=(-0.1 0.9) americanindian_dev=0 asian_dev=0 hispanic_dev=0 other_dev=0 ///
// 							   hpu_stage_w_40_dev=-0.6 hpu_stage_w_20_dev=0.9 hpu_stage_w_30_dev=-0.2 hpu_stage_w_50_dev=-0.05 ///
// 							   black_percent=0.1 american_indian_percent=0 asian_percent=0 hispanic_percent=0 other_percent=0 ///
// 							   hpu_stage_w_40_percentage_wo=0.6  hpu_stage_w_20_percentage_wo=0.1  hpu_stage_w_30_percentage_wo=0.2 ///
// 							   hpu_stage_w_50_percentage_wo=0.05) 
							   
*stage 2	
*calculating predictive reporting rates:
*setting all other variables except race/ulcer severity at mean
*1: white, 2:hispanic, 3:black
foreach black of numlist 1 7{
	foreach stage4 of numlist 6 {
		local black_dev0=-`black'*0.1
		local black_dev1=1-`black'*0.1
		local pblack=`black'*0.1							   
		*predict using logit model
		estimates restore shortstay_logit
		eststo ms2logit`black'`stage4': ///
							   margins, at((means) _all black_dev=(`black_dev0' `black_dev1') americanindian_dev=0 asian_dev=0 hispanic_dev=(-0.01 0.99) other_dev=0 ///
							   hpu_stage_w_40_dev=-0.6 hpu_stage_w_20_dev=0.9 hpu_stage_w_30_dev=-0.2 hpu_stage_w_50_dev=-0.05 ///
							   black_percent=`pblack' american_indian_percent=0 asian_percent=0 hispanic_percent=0.01 other_percent=0 ///
							   hpu_stage_w_40_percentage_wo=0.6  hpu_stage_w_20_percentage_wo=0.1  hpu_stage_w_30_percentage_wo=0.2 ///
							   hpu_stage_w_50_percentage_wo=0.05) post
		*difference between white and hispanic
		lincom _b[1bn._at]-_b[2._at] 
		*difference between white and black
		lincom _b[1bn._at]-_b[3._at]
	}
}

*stage 3			
foreach black of numlist 1 7{
	foreach stage4 of numlist 6 {
		local black_dev0=-`black'*0.1
		local black_dev1=1-`black'*0.1
		local pblack=`black'*0.1
						   
		*predict using logit model
		estimates restore shortstay_logit
		eststo ms3logit`black'`stage4': ///
							   margins, at((means) _all black_dev=(`black_dev0' `black_dev1') americanindian_dev=0 asian_dev=0 hispanic_dev=(-0.01 0.99) other_dev=0 ///
							   hpu_stage_w_40_dev=-0.6 hpu_stage_w_20_dev=-0.1 hpu_stage_w_30_dev=0.8 hpu_stage_w_50_dev=-0.05 ///
							   black_percent=`pblack' american_indian_percent=0 asian_percent=0 hispanic_percent=0.01 other_percent=0 ///
							   hpu_stage_w_40_percentage_wo=0.6  hpu_stage_w_20_percentage_wo=0.1  hpu_stage_w_30_percentage_wo=0.2 ///
							   hpu_stage_w_50_percentage_wo=0.05) post
	}
}			  

*stage 4
foreach black of numlist 1 7{
	foreach stage4 of numlist 6 {
		local black_dev0=-`black'*0.1
		local black_dev1=1-`black'*0.1
		local pblack=`black'*0.1
							   
		*predict using logit model
		estimates restore shortstay_logit
		eststo ms4logit`black'`stage4': ///
							   margins, at((means) _all black_dev=(`black_dev0' `black_dev1') americanindian_dev=0 asian_dev=0 hispanic_dev=(-0.01 0.99) other_dev=0 ///
							   hpu_stage_w_40_dev=0.4 hpu_stage_w_20_dev=-0.1 hpu_stage_w_30_dev=-0.2 hpu_stage_w_50_dev=-0.05 ///
							   black_percent=`pblack' american_indian_percent=0 asian_percent=0 hispanic_percent=0.01 other_percent=0 ///
							   hpu_stage_w_40_percentage_wo=0.6  hpu_stage_w_20_percentage_wo=0.1  hpu_stage_w_30_percentage_wo=0.2 ///
							   hpu_stage_w_50_percentage_wo=0.05) post			
	}
}	

*stage 5
foreach black of numlist 1 7{
	foreach stage4 of numlist 6 {
		local black_dev0=-`black'*0.1
		local black_dev1=1-`black'*0.1
		local pblack=`black'*0.1
							   
		*predict using logit model
		estimates restore shortstay_logit
		eststo ms5logit`black'`stage4': ///
							   margins, at((means) _all black_dev=(`black_dev0' `black_dev1') americanindian_dev=0 asian_dev=0 hispanic_dev=(-0.01 0.99) other_dev=0 ///
							   hpu_stage_w_40_dev=-0.6 hpu_stage_w_20_dev=-0.1 hpu_stage_w_30_dev=-0.2 hpu_stage_w_50_dev=0.95 ///
							   black_percent=`pblack' american_indian_percent=0 asian_percent=0 hispanic_percent=0.01 other_percent=0 ///
							   hpu_stage_w_40_percentage_wo=0.6  hpu_stage_w_20_percentage_wo=0.1  hpu_stage_w_30_percentage_wo=0.2 ///
							   hpu_stage_w_50_percentage_wo=0.05) post		
	}
}

esttab ms2logit16 ms2logit76 ms3logit16 ms3logit76 ms4logit16 ms4logit76 ms5logit16 ms5logit76 ///
using main_shortstay_predictions_fixed_values_raw.csv, nogap nostar ci
* difference of reporting rate between white and black residents
foreach black of numlist 1 7{
	foreach stage4 of numlist 6 {
		estimates restore ml5logit`black'`stage4'
		lincom _b[1bn._at]-_b[2._at]
		lincom _b[1bn._at]-_b[3._at]
	}
}

# other predictions of short-stay pressure ulcer reporting rates
*stage2
foreach black of numlist 1 7{
	foreach stage4 of numlist 4  {
		local black_dev0=-`black'*0.1
		local black_dev1=1-`black'*0.1
		local pblack=`black'*0.1
		local stage2_dev=1-(0.7-`stage4'*0.1)
		local stage4_dev0=0-`stage4'*0.1
		local pstage2=0.7-`stage4'*0.1
		local pstage4=`stage4'*0.1							   
		*predict using logit model
		estimates restore shortstay_logit
		eststo ms2logit`black'`stage4'2: ///
							   margins, at((means) _all black_dev=(`black_dev0' `black_dev1') americanindian_dev=0 asian_dev=0 hispanic_dev=(-0.01 0.99) other_dev=0 ///
							   hpu_stage_w_40_dev=(`stage4_dev0') hpu_stage_w_20_dev=`stage2_dev' hpu_stage_w_30_dev=-0.2 hpu_stage_w_50_dev=-0.05 ///
							   black_percent=`pblack' american_indian_percent=0 asian_percent=0 hispanic_percent=0.01 other_percent=0 ///
							   hpu_stage_w_40_percentage_wo=`pstage4'  hpu_stage_w_20_percentage_wo=`pstage2'  hpu_stage_w_30_percentage_wo=0.2 ///
							   hpu_stage_w_50_percentage_wo=0.05) post
	}
}

*stage3
foreach black of numlist 1 7{
	foreach stage4 of numlist 4 {
		local black_dev0=-`black'*0.1
		local black_dev1=1-`black'*0.1
		local pblack=`black'*0.1
		local stage2_dev=0-(0.7-`stage4'*0.1)
		local stage4_dev0=0-`stage4'*0.1
		local pstage2=0.7-`stage4'*0.1
		local pstage4=`stage4'*0.1							   
		*predict using logit model
		estimates restore shortstay_logit
		eststo ms3logit`black'`stage4'2: ///
							   margins, at((means) _all black_dev=(`black_dev0' `black_dev1') americanindian_dev=0 asian_dev=0 hispanic_dev=(-0.01 0.99) other_dev=0 ///
							   hpu_stage_w_40_dev=(`stage4_dev0') hpu_stage_w_20_dev=`stage2_dev' hpu_stage_w_30_dev=0.8 hpu_stage_w_50_dev=-0.05 ///
							   black_percent=`pblack' american_indian_percent=0 asian_percent=0 hispanic_percent=0.01 other_percent=0 ///
							   hpu_stage_w_40_percentage_wo=`pstage4'  hpu_stage_w_20_percentage_wo=`pstage2'  hpu_stage_w_30_percentage_wo=0.2 ///
							   hpu_stage_w_50_percentage_wo=0.05) post
	}
}


*stage 4						  						  

foreach black of numlist 1 7{
	foreach stage4 of numlist 4 6 {
		local black_dev0=-`black'*0.1
		local black_dev1=1-`black'*0.1
		local pblack=`black'*0.1
		local stage2_dev=0-(0.7-`stage4'*0.1)
		local stage4_dev1=1-`stage4'*0.1
		local pstage2=0.7-`stage4'*0.1
		local pstage4=`stage4'*0.1							   
		*predict using logit model
		estimates restore shortstay_logit
		eststo ms4logit`black'`stage4'2: ///
							   margins, at((means) _all black_dev=(`black_dev0' `black_dev1') americanindian_dev=0 asian_dev=0 hispanic_dev=(-0.01 0.99) other_dev=0 ///
							   hpu_stage_w_40_dev=(`stage4_dev1') hpu_stage_w_20_dev=`stage2_dev' hpu_stage_w_30_dev=-0.2 hpu_stage_w_50_dev=-0.05 ///
							   black_percent=`pblack' american_indian_percent=0 asian_percent=0 hispanic_percent=0.01 other_percent=0 ///
							   hpu_stage_w_40_percentage_wo=`pstage4'  hpu_stage_w_20_percentage_wo=`pstage2'  hpu_stage_w_30_percentage_wo=0.2 ///
							   hpu_stage_w_50_percentage_wo=0.05) post					   
	}
}	

// esttab ms4logit142 ms4logit162 ms4logit742 ms4logit762, nogap nostar
			  
*Unstageable						  						  

foreach black of numlist 1 7{
	foreach stage4 of numlist 4 {
		local black_dev0=-`black'*0.1
		local black_dev1=1-`black'*0.1
		local pblack=`black'*0.1
		local stage2_dev=0-(0.7-`stage4'*0.1)
		local stage4_dev0=0-`stage4'*0.1
		local pstage2=0.7-`stage4'*0.1
		local pstage4=`stage4'*0.1							   
		*predict using logit model
		estimates restore shortstay_logit
		eststo ms5logit`black'`stage4'2: ///
							   margins, at((means) _all black_dev=(`black_dev0' `black_dev1') americanindian_dev=0 asian_dev=0 hispanic_dev=(-0.01 0.99) other_dev=0 ///
							   hpu_stage_w_40_dev=(`stage4_dev0') hpu_stage_w_20_dev=`stage2_dev' hpu_stage_w_30_dev=-0.2 hpu_stage_w_50_dev=0.95 ///
							   black_percent=`pblack' american_indian_percent=0 asian_percent=0 hispanic_percent=0.01 other_percent=0 ///
							   hpu_stage_w_40_percentage_wo=`pstage4'  hpu_stage_w_20_percentage_wo=`pstage2'  hpu_stage_w_30_percentage_wo=0.2 ///
							   hpu_stage_w_50_percentage_wo=0.05) post		
	}
}		
esttab ms2logit142 ms2logit742 ms3logit142 ms3logit742 ms4logit142 ms4logit742 ms5logit142 ms5logit742 using main_shortstay_predictions_fixed_values_other_raw.csv, nogap nostar ci

foreach black of numlist 1 7{
	foreach stage4 of numlist 4 {
		estimates restore ml5logit`black'`stage4'2
		lincom _b[1bn._at]-_b[2._at]
		lincom _b[1bn._at]-_b[3._at]
	}
}
			  
*-------*-------*-------*-------*-------*-------*-------*-------*-------*-------*-------*-------*-------;

*-------*-------*-------*-------*-------*-------*-------*-------*-------*-------*-------*-------*-------;
*long-stay
eststo longstay_logit: melogit m_pu $race_dev $race_percentage $higheststage_dev $higheststage_percentage ///
						$level1_interaction $level2_interaction $other ///
						if short_stay_n==1 || prvdrnum:
esttab longstay_logit using main_longstaylogit_regression.csv, se nogap label replace
						
// estimates restore longstay_logit
// eststo mllogit_race: margins, over(race_n) post
// esttab mllogit_race, label ci nogap

* CALCULATE PREDICTIVE REPORTING RATES AT EACH STAGE FOR LONG-STAY RESIDENTS

*stage 2			
foreach black of numlist 1 7{
	foreach stage4 of numlist 6 {
		local black_dev0=-`black'*0.1
		local black_dev1=1-`black'*0.1
		local pblack=`black'*0.1							   
		*predict using logit model
		estimates restore longstay_logit
		eststo ml2logit`black'`stage4': ///
							   margins, at((means) _all black_dev=(`black_dev0' `black_dev1') americanindian_dev=0 asian_dev=0 hispanic_dev=(-0.01 0.99) other_dev=0 ///
							   hpu_stage_w_40_dev=-0.6 hpu_stage_w_20_dev=0.9 hpu_stage_w_30_dev=-0.2 hpu_stage_w_50_dev=-0.05 ///
							   black_percent=`pblack' american_indian_percent=0 asian_percent=0 hispanic_percent=0.01 other_percent=0 ///
							   hpu_stage_w_40_percentage_wo=0.6  hpu_stage_w_20_percentage_wo=0.1  hpu_stage_w_30_percentage_wo=0.2 ///
							   hpu_stage_w_50_percentage_wo=0.05) post
	}
}

*stage 3			
foreach black of numlist 1 7{
	foreach stage4 of numlist 6 {
		local black_dev0=-`black'*0.1
		local black_dev1=1-`black'*0.1
		local pblack=`black'*0.1
						   
		*predict using logit model
		estimates restore longstay_logit
		eststo ml3logit`black'`stage4': ///
							   margins, at((means) _all black_dev=(`black_dev0' `black_dev1') americanindian_dev=0 asian_dev=0 hispanic_dev=(-0.01 0.99) other_dev=0 ///
							   hpu_stage_w_40_dev=-0.6 hpu_stage_w_20_dev=-0.1 hpu_stage_w_30_dev=0.8 hpu_stage_w_50_dev=-0.05 ///
							   black_percent=`pblack' american_indian_percent=0 asian_percent=0 hispanic_percent=0.01 other_percent=0 ///
							   hpu_stage_w_40_percentage_wo=0.6  hpu_stage_w_20_percentage_wo=0.1  hpu_stage_w_30_percentage_wo=0.2 ///
							   hpu_stage_w_50_percentage_wo=0.05) post
		lincom _b[1bn._at]-_b[2._at]
	}
}			  

*stage 4
foreach black of numlist 1 7{
	foreach stage4 of numlist 6 {
		local black_dev0=-`black'*0.1
		local black_dev1=1-`black'*0.1
		local pblack=`black'*0.1
							   
		*predict using logit model
		estimates restore longstay_logit
		eststo ml4logit`black'`stage4': ///
							   margins, at((means) _all black_dev=(`black_dev0' `black_dev1') americanindian_dev=0 asian_dev=0 hispanic_dev=(-0.01 0.99) other_dev=0 ///
							   hpu_stage_w_40_dev=0.4 hpu_stage_w_20_dev=-0.1 hpu_stage_w_30_dev=-0.2 hpu_stage_w_50_dev=-0.05 ///
							   black_percent=`pblack' american_indian_percent=0 asian_percent=0 hispanic_percent=0.01 other_percent=0 ///
							   hpu_stage_w_40_percentage_wo=0.6  hpu_stage_w_20_percentage_wo=0.1  hpu_stage_w_30_percentage_wo=0.2 ///
							   hpu_stage_w_50_percentage_wo=0.05) post		
	}
}	

*stage 5
foreach black of numlist 1 7{
	foreach stage4 of numlist 6 {
		local black_dev0=-`black'*0.1
		local black_dev1=1-`black'*0.1
		local pblack=`black'*0.1
							   
		*predict using logit model
		estimates restore longstay_logit
		eststo ml5logit`black'`stage4': ///
							   margins, at((means) _all black_dev=(`black_dev0' `black_dev1') americanindian_dev=0 asian_dev=0 hispanic_dev=(-0.01 0.99) other_dev=0 ///
							   hpu_stage_w_40_dev=-0.6 hpu_stage_w_20_dev=-0.1 hpu_stage_w_30_dev=-0.2 hpu_stage_w_50_dev=0.95 ///
							   black_percent=`pblack' american_indian_percent=0 asian_percent=0 hispanic_percent=0.01 other_percent=0 ///
							   hpu_stage_w_40_percentage_wo=0.6  hpu_stage_w_20_percentage_wo=0.1  hpu_stage_w_30_percentage_wo=0.2 ///
							   hpu_stage_w_50_percentage_wo=0.05) post	
	}
}

esttab ml2logit16 ml2logit76 ml3logit16 ml3logit76 ml4logit16 ml4logit76 ml5logit16 ml5logit76 ///
using main_longstay_predictions_fixed_values_raw.csv, nostar nogap ci					   

* other predictions of reporting rates for long-stay residents
* the percentage of stage 4 pressure ulcer is changed to 40%
*stage2


foreach black of numlist 1 7{
	foreach stage4 of numlist 4 {
		local black_dev0=-`black'*0.1
		local black_dev1=1-`black'*0.1
		local pblack=`black'*0.1
		local stage2_dev=1-(0.7-`stage4'*0.1)
		local stage4_dev0=0-`stage4'*0.1
		local pstage2=0.7-`stage4'*0.1
		local pstage4=`stage4'*0.1							   
		*predict using logit model
		estimates restore longstay_logit
		eststo ml2logit`black'`stage4'2: ///
							   margins, at((means) _all black_dev=(`black_dev0' `black_dev1') americanindian_dev=0 asian_dev=0 hispanic_dev=(-0.01 0.99) other_dev=0 ///
							   hpu_stage_w_40_dev=(`stage4_dev0') hpu_stage_w_20_dev=`stage2_dev' hpu_stage_w_30_dev=-0.2 hpu_stage_w_50_dev=-0.05 ///
							   black_percent=`pblack' american_indian_percent=0 asian_percent=0 hispanic_percent=0.01 other_percent=0 ///
							   hpu_stage_w_40_percentage_wo=`pstage4'  hpu_stage_w_20_percentage_wo=`pstage2'  hpu_stage_w_30_percentage_wo=0.2 ///
							   hpu_stage_w_50_percentage_wo=0.05) post
	}
}

*stage3
foreach black of numlist 1 7{
	foreach stage4 of numlist 4 {
		local black_dev0=-`black'*0.1
		local black_dev1=1-`black'*0.1
		local pblack=`black'*0.1
		local stage2_dev=0-(0.7-`stage4'*0.1)
		local stage4_dev0=0-`stage4'*0.1
		local pstage2=0.7-`stage4'*0.1
		local pstage4=`stage4'*0.1							   
		*predict using logit model
		estimates restore longstay_logit
		eststo ml3logit`black'`stage4'2: ///
							   margins, at((means) _all black_dev=(`black_dev0' `black_dev1') americanindian_dev=0 asian_dev=0 hispanic_dev=(-0.01 0.99) other_dev=0 ///
							   hpu_stage_w_40_dev=(`stage4_dev0') hpu_stage_w_20_dev=`stage2_dev' hpu_stage_w_30_dev=0.8 hpu_stage_w_50_dev=-0.05 ///
							   black_percent=`pblack' american_indian_percent=0 asian_percent=0 hispanic_percent=0.01 other_percent=0 ///
							   hpu_stage_w_40_percentage_wo=`pstage4'  hpu_stage_w_20_percentage_wo=`pstage2'  hpu_stage_w_30_percentage_wo=0.2 ///
							   hpu_stage_w_50_percentage_wo=0.05) post
	}
}

//
// esttab mslogit14 mslogit15 mslogit16 mslogit17 mslogit34 mslogit35 mslogit36 mslogit37, ci nogap
// esttab mslogit54 mslogit55 mslogit56 mslogit57 mslogit74 mslogit75 mslogit76 mslogit77, ci nogap
//

*stage 4						  						  
foreach black of numlist 1 7{
	foreach stage4 of numlist 4 6 {
		local black_dev0=-`black'*0.1
		local black_dev1=1-`black'*0.1
		local pblack=`black'*0.1
		local stage2_dev=0-(0.7-`stage4'*0.1)
		local stage4_dev1=1-`stage4'*0.1
		local pstage2=0.7-`stage4'*0.1
		local pstage4=`stage4'*0.1							   
		*predict using logit model
		estimates restore longstay_logit
		eststo ml4logit`black'`stage4'2: ///
							   margins, at((means) _all black_dev=(`black_dev0' `black_dev1') americanindian_dev=0 asian_dev=0 hispanic_dev=(-0.01 0.99) other_dev=0 ///
							   hpu_stage_w_40_dev=(`stage4_dev1') hpu_stage_w_20_dev=`stage2_dev' hpu_stage_w_30_dev=-0.2 hpu_stage_w_50_dev=-0.05 ///
							   black_percent=`pblack' american_indian_percent=0 asian_percent=0 hispanic_percent=0.01 other_percent=0 ///
							   hpu_stage_w_40_percentage_wo=`pstage4'  hpu_stage_w_20_percentage_wo=`pstage2'  hpu_stage_w_30_percentage_wo=0.2 ///
							   hpu_stage_w_50_percentage_wo=0.05) post				   
	}
}	

			  
*Unstageable						  						  

foreach black of numlist 1 7{
	foreach stage4 of numlist 4 {
		local black_dev0=-`black'*0.1
		local black_dev1=1-`black'*0.1
		local pblack=`black'*0.1
		local stage2_dev=0-(0.7-`stage4'*0.1)
		local stage4_dev0=0-`stage4'*0.1
		local pstage2=0.7-`stage4'*0.1
		local pstage4=`stage4'*0.1							   
		*predict using logit model
		estimates restore longstay_logit
		eststo ml5logit`black'`stage4'2: ///
							   margins, at((means) _all black_dev=(`black_dev0' `black_dev1') americanindian_dev=0 asian_dev=0 hispanic_dev=(-0.01 0.99) other_dev=0 ///
							   hpu_stage_w_40_dev=(`stage4_dev0') hpu_stage_w_20_dev=`stage2_dev' hpu_stage_w_30_dev=-0.2 hpu_stage_w_50_dev=0.95 ///
							   black_percent=`pblack' american_indian_percent=0 asian_percent=0 hispanic_percent=0.01 other_percent=0 ///
							   hpu_stage_w_40_percentage_wo=`pstage4'  hpu_stage_w_20_percentage_wo=`pstage2'  hpu_stage_w_30_percentage_wo=0.2 ///
							   hpu_stage_w_50_percentage_wo=0.05) post		
	}
}
				  
esttab ml2logit142 ml2logit742 ml3logit142 ml3logit742 ml4logit142 ml4logit742 ml5logit142 ml5logit742 using main_longstay_predictions_fixed_values_other_raw.csv, nogap nostar ci