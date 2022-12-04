#!/usr/bin/env python3

import os
import sys
import shutil
import argparse
import traceback
from pathlib import Path
import json
import ast
import pandas as pd
from rasterio.errors import RasterioIOError

from tools_shared_functions import compute_contingency_stats_from_rasters
from tools_shared_variables import (TEST_CASES_DIR, INPUTS_DIR, ENDC, TRED_BOLD, WHITE_BOLD, CYAN_BOLD, AHPS_BENCHMARK_CATEGORIES, IFC_MAGNITUDE_LIST, BLE_MAGNITUDE_LIST )
from inundation import inundate
from gms_tools.inundate_gms import Inundate_gms
from gms_tools.mosaic_inundation import Mosaic_inundation, mosaic_by_unit
from gms_tools.overlapping_inundation import OverlapWindowMerge
from glob import glob
from utils.shared_variables import elev_raster_ndv

def run_alpha_test( fim_run_dir, version, test_id, magnitude, 
                calibrated, model,
                all_huc12s_in_current_huc=None,
                last_huc12=None,
                compare_to_previous=False, archive_results=False, 
                mask_type='filter', inclusion_area='', 
                inclusion_area_buffer=0, light_run=False, 
                overwrite=False, fr_run_dir=None, 
                gms_workers=1,verbose=False,
                keep_gms=False,
                gms_verbose=False
              ):

    # check eval_meta input
    if model not in {None,'FR','MS','GMS'}:
        raise ValueError("Model argument needs to be \'FR\', \'MS\', or \'GMS.\'")

    huc12 = False
    if len(os.path.basename(fim_run_dir)) == 12:
        huc12 = True

    # make bool
    calibrated = bool( calibrated )

    if (model == "MS") & (fr_run_dir is None):
        raise ValueError("fr_run_dir argument needs to be specified with MS model")

    benchmark_category = test_id.split('_')[1] # Parse benchmark_category from test_id.
    current_huc = test_id.split('_')[0]  # Break off HUC ID and assign to variable.
    current_huc12 = os.path.basename(fim_run_dir)

    # Construct paths to development test results if not existent.
    if archive_results:
        version_test_case_dir_parent = os.path.join(TEST_CASES_DIR, benchmark_category + '_test_cases', test_id, 'official_versions', version)
    else:
        version_test_case_dir_parent = os.path.join(TEST_CASES_DIR, benchmark_category + '_test_cases', test_id, 'testing_versions', version)

    # Delete the entire directory if it already exists.
    if os.path.exists(version_test_case_dir_parent):
        if overwrite & (not huc12):
            shutil.rmtree(version_test_case_dir_parent,ignore_errors=True)
        elif model == 'MS':
            pass
        elif huc12:
            pass
        else:
            print("Metrics for ({version}: {test_id}) already exist. Use overwrite flag (-o) to overwrite metrics.".format(version=version, test_id=test_id))
            return

    os.makedirs(version_test_case_dir_parent,exist_ok=True)

    __vprint("Running the alpha test for test_id: " + test_id + ", " + version + "...",verbose)
    stats_modes_list = ['total_area']

    fim_run_parent = os.path.join(os.environ['outputDataDir'], fim_run_dir)
    assert os.path.exists(fim_run_parent), "Cannot locate " + fim_run_parent

    # get hydrofabric directory
    hydrofabric_dir = Path(fim_run_parent).parent.absolute()

    # Create paths to fim_run outputs for use in inundate().
    rem = os.path.join(fim_run_parent, 'rem_zeroed_masked.tif')
    if not os.path.exists(rem):
        rem = os.path.join(fim_run_parent, 'rem_clipped_zeroed_masked.tif')
    catchments = os.path.join(fim_run_parent, 'gw_catchments_reaches_filtered_addedAttributes.tif')
    if not os.path.exists(catchments):
        catchments = os.path.join(fim_run_parent, 'gw_catchments_reaches_clipped_addedAttributes.tif')
    if mask_type == 'huc':
        catchment_poly = ''
    else:
        catchment_poly = os.path.join(fim_run_parent, 'gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg')
    hydro_table = os.path.join(fim_run_parent, 'hydroTable.csv')

    # Map necessary inputs for inundation().
    hucs, hucs_layerName = os.path.join(INPUTS_DIR, 'wbd', 'WBD_National.gpkg'), 'WBDHU8'

    # Create list of shapefile paths to use as exclusion areas.
    zones_dir = os.path.join(TEST_CASES_DIR, 'other', 'zones')
    mask_dict = {'levees':
                    {'path': os.path.join(zones_dir, 'leveed_areas_conus.shp'),
                     'buffer': None,
                     'operation': 'exclude'
                     },
                'waterbodies':
                    {'path': os.path.join(zones_dir, 'nwm_v2_reservoirs.shp'),
                     'buffer': None,
                     'operation': 'exclude',
                     },
                }

    if inclusion_area != '':
        inclusion_area_name = os.path.split(inclusion_area)[1].split('.')[0]  # Get layer name
        mask_dict.update({inclusion_area_name: {'path': inclusion_area,
                                                'buffer': int(inclusion_area_buffer),
                                                'operation': 'include'}})
        # Append the concatenated inclusion_area_name and buffer.
        if inclusion_area_buffer == None:
            inclusion_area_buffer = 0
        stats_modes_list.append(inclusion_area_name + '_b' + str(inclusion_area_buffer) + 'm')

    # Check if magnitude is list of magnitudes or single value.
    magnitude_list = magnitude
    if type(magnitude_list) != list:
        magnitude_list = [magnitude_list]
        

    # Get path to validation_data_{benchmark} directory and huc_dir.
    validation_data_path = os.path.join(TEST_CASES_DIR, benchmark_category + '_test_cases', 'validation_data_' + benchmark_category)
    for magnitude in magnitude_list:
        version_test_case_dir = os.path.join(version_test_case_dir_parent, magnitude)
        if not os.path.exists(version_test_case_dir):
            os.mkdir(version_test_case_dir)
        # Construct path to validation raster and forecast file.
        if benchmark_category in AHPS_BENCHMARK_CATEGORIES:
            benchmark_raster_path_list, forecast_list = [], []
            lid_dir_list = os.listdir(os.path.join(validation_data_path, current_huc))
            lid_list, inundation_raster_list, domain_file_list = [], [], []

            for lid in lid_dir_list:
                lid_dir = os.path.join(validation_data_path, current_huc, lid)
                benchmark_lid_raster_path = os.path.join(lid_dir, magnitude, 'ahps_' + lid + '_huc_' + current_huc + '_extent_' + magnitude + '.tif')

                # Only compare if the benchmark data exist.
                if os.path.exists(benchmark_lid_raster_path):
                    benchmark_raster_path_list.append(benchmark_lid_raster_path)  # TEMP
                    forecast_list.append(os.path.join(lid_dir, magnitude, 'ahps_' + lid + '_huc_' + current_huc + '_flows_' + magnitude + '.csv'))  # TEMP
                    lid_list.append(lid)
                    inundation_raster_list.append(os.path.join(version_test_case_dir, lid + '_inundation_extent.tif'))
                    domain_file_list.append(os.path.join(lid_dir, lid + '_domain.shp'))

        else:
            benchmark_raster_file = os.path.join(TEST_CASES_DIR, benchmark_category + '_test_cases', 'validation_data_' + benchmark_category, current_huc, magnitude, benchmark_category + '_huc_' + current_huc + '_extent_' + magnitude + '.tif')
            benchmark_raster_path_list = [benchmark_raster_file]
            forecast_path = os.path.join(TEST_CASES_DIR, benchmark_category + '_test_cases', 'validation_data_' + benchmark_category, current_huc, magnitude, benchmark_category + '_huc_' + current_huc + '_flows_' + magnitude + '.csv')
            forecast_list = [forecast_path]

            # make temp inundation
            if huc12:
                inundation_raster_list = [os.path.join(version_test_case_dir, f'inundation_extent_huc12_{current_huc12}.tif')]
            else:
                inundation_raster_list = [os.path.join(version_test_case_dir, 'inundation_extent.tif')]
            
        for index in range(0, len(benchmark_raster_path_list)):
            benchmark_raster_path = benchmark_raster_path_list[index]
            forecast = forecast_list[index]
            inundation_raster = inundation_raster_list[index]
            # Only need to define ahps_lid and ahps_extent_file for AHPS_BENCHMARK_CATEGORIES.
            if benchmark_category in AHPS_BENCHMARK_CATEGORIES:
                ahps_lid = lid_list[index]
                ahps_domain_file = domain_file_list[index]
                mask_dict.update({ahps_lid:
                    {'path': ahps_domain_file,
                     'buffer': None,
                     'operation': 'include'}
                        })

                if not os.path.exists(benchmark_raster_path) or not os.path.exists(ahps_domain_file) or not os.path.exists(forecast):  # Skip loop instance if the benchmark raster doesn't exist.
                    continue
            else:  # If not in AHPS_BENCHMARK_CATEGORIES.
                if not os.path.exists(benchmark_raster_path) or not os.path.exists(forecast):  # Skip loop instance if the benchmark raster doesn't exist.
                    continue
            # Run inundate.
            __vprint("-----> Running inundate() to produce inundation extent for the " + magnitude + " magnitude...",verbose)
            # The inundate adds the huc to the name so I account for that here.
            if huc12:
                predicted_raster_path = inundation_raster
            else:
                predicted_raster_path = os.path.join(
                                            os.path.split(inundation_raster)[0], 
                                            os.path.split(inundation_raster)[1].replace('.tif', '_'+current_huc+'.tif')
                                                    )  
            
            # assign current huc variable for inundation mapping
            if huc12:
                ch = current_huc12
            else:
                ch = current_huc
                    
            try:
                if model == 'GMS':
                    
                    map_file = Inundate_gms(
                                             hydrofabric_dir=hydrofabric_dir, 
                                             forecast=forecast, 
                                             num_workers=gms_workers,
                                             hucs=ch,
                                             inundation_raster=inundation_raster,
                                             inundation_polygon=None, depths_raster=None,
                                             verbose=gms_verbose,
                                             log_file=None,
                                             output_fileNames=None
                                            )
                    
                    mask_path_gms = os.path.join(fim_run_parent, 'wbd.gpkg')

                    Mosaic_inundation(
                                        map_file,mosaic_attribute='inundation_rasters',
                                        mosaic_output=inundation_raster,
                                        mask=mask_path_gms,unit_attribute_name='huc8',
                                        nodata=elev_raster_ndv,workers=1,
                                        remove_inputs=(not keep_gms),
                                        subset=None,verbose=verbose
                                      )
                
                else:
                    inundate(
                             rem,catchments,catchment_poly,hydro_table,forecast,
                             mask_type,hucs=hucs,hucs_layerName=hucs_layerName,
                             subset_hucs=current_huc,num_workers=1,aggregate=False,
                             inundation_raster=inundation_raster,inundation_polygon=None,
                             depths=None,out_raster_profile=None,out_vector_profile=None,
                             quiet=True
                        )

                if model =='MS':
                    
                    # Mainstems inundation
                    #fr_run_parent = os.path.join(os.environ['outputDataDir'], fr_run_dir,current_huc)
                    #assert os.path.exists(fr_run_parent), "Cannot locate " + fr_run_parent
                    
                    inundation_raster_ms = os.path.join(
                                        os.path.split(inundation_raster)[0], 
                                        os.path.split(inundation_raster)[1].replace('.tif', '_{}_MS.tif'.format(current_huc))
                                           )  
                    inundation_raster_fr = os.path.join(
                                        os.path.split(version_test_case_dir_parent)[0],
                                        fr_run_dir,
                                        magnitude,
                                        os.path.split(inundation_raster)[1].replace('.tif', '_'+current_huc+'.tif')
                                           )  
                    
                    os.rename(predicted_raster_path,inundation_raster_ms)

                    ms_inundation_map_file = { 
                                               'huc8' : [current_huc] * 2,
                                               'branchID' : [None] * 2,
                                               'inundation_rasters' : [inundation_raster_fr,inundation_raster_ms],
                                               'depths_rasters' : [None] * 2,
                                               'inundation_polygons' : [None] * 2
                                             }
                    ms_inundation_map_file = pd.DataFrame(ms_inundation_map_file)
                    
                    Mosaic_inundation(
                                        ms_inundation_map_file,mosaic_attribute='inundation_rasters',
                                        mosaic_output=inundation_raster,
                                        mask=catchment_poly,unit_attribute_name='huc8',
                                        nodata=elev_raster_ndv,workers=1,
                                        remove_inputs=False,
                                        subset=None,verbose=verbose
                                      )

                __vprint("-----> Inundation mapping complete.",verbose)

                # Define outputs for agreement_raster, stats_json, and stats_csv.
                if benchmark_category in AHPS_BENCHMARK_CATEGORIES:
                    agreement_raster, stats_json, stats_csv = os.path.join(version_test_case_dir, lid + 'total_area_agreement.tif'), os.path.join(version_test_case_dir, 'stats.json'), os.path.join(version_test_case_dir, 'stats.csv')
                else:
                    agreement_raster, stats_json, stats_csv = os.path.join(version_test_case_dir, 'total_area_agreement.tif'), os.path.join(version_test_case_dir, 'stats.json'), os.path.join(version_test_case_dir, 'stats.csv')
                
                # aggregate predicted rasters if huc12
                if huc12:
                    # determine if all huc12s for current huc8 have been computed
                    computed_huc12s = glob(os.path.join(version_test_case_dir,'inundation_extent_huc12_*'))
                    computed_huc12s_set = set(
                                [os.path.basename(huc12_file).split('_')[3] for huc12_file in computed_huc12s]
                                         )
                    
                    all_computed_bool = [True if h12 in computed_huc12s_set else False for h12 in all_huc12s_in_current_huc ]
                    all_computed = all(all_computed_bool)

                    if all_computed:
                        print(current_huc,current_huc12,last_huc12,all_computed,magnitude)

                    if all_computed & (last_huc12 == current_huc12):
                        
                        predicted_raster_path = os.path.join(
                                                             os.path.split(inundation_raster)[0], 
                                                             'inundation_extent_'+current_huc+'.tif'
                                                            )
                        # try to mosaic until no errors (all jobs finish)
                        while True:
                            try:
                                mosaic_by_unit(
                                               computed_huc12s,
                                               predicted_raster_path,nodata=elev_raster_ndv,
                                               workers=1,remove_inputs=False,mask=None,verbose=False
                                              )
                                break
                            except RasterioIOError:
                                continue

                    else:
                        continue

                compute_contingency_stats_from_rasters(predicted_raster_path,
                                                       benchmark_raster_path,
                                                       agreement_raster,
                                                       stats_csv=stats_csv,
                                                       stats_json=stats_json,
                                                       mask_values=[],
                                                       stats_modes_list=stats_modes_list,
                                                       test_id=test_id,
                                                       mask_dict=mask_dict,
                                                       )

                if benchmark_category in AHPS_BENCHMARK_CATEGORIES:
                    del mask_dict[ahps_lid]

                __vprint(" ",verbose)
                # print("Evaluation complete. All metrics for " + test_id + ", " + version + ", " + magnitude + " are available at " + CYAN_BOLD + version_test_case_dir + ENDC) # GMS
                __vprint("Evaluation metrics for " + test_id + ", " + version + ", " + magnitude + " are available at " + CYAN_BOLD + version_test_case_dir + ENDC,verbose) # cahaba/dev
                __vprint(" ",verbose)

            except Exception as e:
                print(traceback.print_exc())
                #print(e)

        if benchmark_category in AHPS_BENCHMARK_CATEGORIES:
            # -- Delete temp files -- #
            # List all files in the output directory.
            output_file_list = os.listdir(version_test_case_dir)
            for output_file in output_file_list:
                if "total_area" in output_file:
                    full_output_file_path = os.path.join(version_test_case_dir, output_file)
                    os.remove(full_output_file_path)

    # write out evaluation meta-data
    with open(os.path.join(version_test_case_dir_parent,'eval_metadata.json'),'w') as meta:
        eval_meta = { 'calibrated' : calibrated , 'model' : model }
        meta.write( 
                    json.dumps(eval_meta,indent=2) 
                   )

def __vprint(message,verbose):
    if verbose:
        print(message)


if __name__ == '__main__':

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Inundation mapping and regression analysis for FOSS FIM. Regression analysis results are stored in the test directory.')
    parser.add_argument('-r','--fim-run-dir',help='Name of directory containing outputs of fim_run.sh',required=True)
    parser.add_argument('-b', '--version',help='The name of the working version in which features are being tested',required=True,default="")
    parser.add_argument('-t', '--test-id',help='The test_id to use. Format as: HUC_BENCHMARKTYPE, e.g. 12345678_ble.',required=True,default="")
    parser.add_argument('-m', '--mask-type', help='Specify \'huc\' (FIM < 3) or \'filter\' (FIM >= 3) masking method. MS and GMS are currently on supporting huc', required=False,default="filter")
    parser.add_argument('-n','--calibrated',help='Denotes use of calibrated n values',required=False, default=False,action='store_true')
    parser.add_argument('-e','--model',help='Denotes model used. FR, MS, or GMS allowed',required=True)
    parser.add_argument('-y', '--magnitude',help='The magnitude to run.',required=False, default="")
    parser.add_argument('-c', '--compare-to-previous', help='Compare to previous versions of HAND.', required=False,action='store_true')
    parser.add_argument('-a', '--archive-results', help='Automatically copy results to the "previous_version" archive for test_id. For admin use only.', required=False,action='store_true')
    parser.add_argument('-i', '--inclusion-area', help='Path to shapefile. Contingency metrics will be produced from pixels inside of shapefile extent.', required=False, default="")
    parser.add_argument('-ib','--inclusion-area-buffer', help='Buffer to use when masking contingency metrics with inclusion area.', required=False, default="0")
    parser.add_argument('-l', '--light-run', help='Using the light_run option will result in only stat files being written, and NOT grid files.', required=False, action='store_true')
    parser.add_argument('-o','--overwrite',help='Overwrite all metrics or only fill in missing metrics.',required=False, default=False, action='store_true')
    parser.add_argument('-kg','--keep-gms',help='Keeps branch level inundation',required=False, default=False, action='store_true')
    parser.add_argument('-w','--gms-workers', help='Number of workers to use for GMS Branch Inundation', required=False, default=1)
    parser.add_argument('-d','--fr-run-dir',help='Name of test case directory containing inundation for FR configuration',required=False,default=None)
    parser.add_argument('-vr', '--verbose', help='Verbose operation', required=False, action='store_true', default=False)
    parser.add_argument('-vg', '--gms-verbose', help='Prints progress bar for GMS', required=False, action='store_true', default=False)

    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    valid_test_id_list = os.listdir(TEST_CASES_DIR)

    exit_flag = False  # Default to False.
    __vprint("",args['verbose'])

    # Ensure test_id is valid.
#    if args['test_id'] not in valid_test_id_list:
#        print(TRED_BOLD + "Warning: " + WHITE_BOLD + "The provided test_id (-t) " + CYAN_BOLD + args['test_id'] + WHITE_BOLD + " is not available." + ENDC)
#        print(WHITE_BOLD + "Available test_ids include: " + ENDC)
#        for test_id in valid_test_id_list:
#          if 'validation' not in test_id.split('_') and 'ble' in test_id.split('_'):
#              print(CYAN_BOLD + test_id + ENDC)
#        print()
#        exit_flag = True

    # Ensure fim_run_dir exists.
    if not os.path.exists(os.path.join(os.environ['outputDataDir'], args['fim_run_dir'])):
        print(TRED_BOLD + "Warning: " + WHITE_BOLD + "The provided fim_run_dir (-r) " + CYAN_BOLD + args['fim_run_dir'] + WHITE_BOLD + " could not be located in the 'outputs' directory." + ENDC)
        print(WHITE_BOLD + "Please provide the parent directory name for fim_run.sh outputs. These outputs are usually written in a subdirectory, e.g. outputs/123456/123456." + ENDC)
        print()
        exit_flag = True

    # Ensure inclusion_area path exists.
    if args['inclusion_area'] != "" and not os.path.exists(args['inclusion_area']):
        print(TRED_BOLD + "Error: " + WHITE_BOLD + "The provided inclusion_area (-i) " + CYAN_BOLD + args['inclusion_area'] + WHITE_BOLD + " could not be located." + ENDC)
        exit_flag = True

    try:
        inclusion_buffer = int(args['inclusion_area_buffer'])
    except ValueError:
        print(TRED_BOLD + "Error: " + WHITE_BOLD + "The provided inclusion_area_buffer (-ib) " + CYAN_BOLD + args['inclusion_area_buffer'] + WHITE_BOLD + " is not a round number." + ENDC)

    benchmark_category = args['test_id'].split('_')[1]

    if args['magnitude'] == '':
        if 'ble' == benchmark_category:
            args['magnitude'] = BLE_MAGNITUDE_LIST
        elif ('nws' == benchmark_category) | ('usgs' == benchmark_category):
            args['magnitude'] = ['action', 'minor', 'moderate', 'major']
        elif 'ifc' == current_benchmark_category:
            args['magnitude'] = IFC_MAGNITUDE_LIST
        else:
            print(TRED_BOLD + "Error: " + WHITE_BOLD + "The provided magnitude (-y) " + CYAN_BOLD + args['magnitude'] + WHITE_BOLD + " is invalid. ble options include: 100yr, 500yr. ahps options include action, minor, moderate, major." + ENDC)
            exit_flag = True

    if exit_flag:
        print()
        sys.exit()

    else:
        run_alpha_test(**args)
