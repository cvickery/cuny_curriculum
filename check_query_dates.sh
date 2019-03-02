#! /usr/local/bin/bash
# Link the latest version of each query in queries into the latest_queries folder, dropping the
# CUNYfirst run-id part of the file name. Archive old queries if all the dates are ok.

# Subshell to ensuring proper directories
(
  cd /Users/vickery/CUNY_Courses

  skip_size_test=''
  if [[ $# > 1 ]]
  then  echo 'Usage: $0 [-s | --skip-size-test]'
        exit -1
  fi
  if [[ $# > 0 ]]
  then  if [[ $1 != '-s' && $1 != '--skip-size-test' ]]
        then echo "Invalid option: $1"
             echo 'Usage: $0 [-s | --skip-size-test]'
             exit -1
        fi
          skip_size_test='skip'
  fi
  # Clear the latest_queries folder
  rm -f latest_queries/*

  for query in  ACAD_CAREER_TBL QCCV_RQMNT_DESIG_TBL QNS_CV_ACADEMIC_ORGANIZATIONS \
                QNS_CV_CUNY_SUBJECTS QNS_CV_CUNY_SUBJECT_TABLE \
                QNS_CV_SR_TRNS_INTERNAL_RULES QNS_QCCV_COURSE_ATTRIBUTES_NP QNS_QCCV_CU_CATALOG_NP \
                QNS_QCCV_CU_REQUISITES_NP SR701____INSTITUTION_TABLE SR742A___CRSE_ATTRIBUTE_VALUE \
                QNS_CV_CRSE_EQUIV_TBL
  do
    all=(`ls -t queries/${query}*`)
    first=${all[0]}
    t=${first%-*}
    t=latest_$t
    ln $first $t.csv
    gstat -c "%w %n" $t.csv | cut -c 1-11,52-
  done

  # Do the query dates match?
  dates=()
  for file in latest_queries/*.csv
  do
    dates+=(`gstat -c %w $file | cut -c 1-10`)
  done
  for d1 in ${dates[@]}
  do
    for d2 in ${dates[@]}
    do
      if [[ $d1 != $d2 ]]
      then echo "Query dates do not match: $d1 != $d2"
      exit 1
      fi
    done
  done
  echo "OK: All queries are dated ${dates[0]}" >&2

  # Check file sizes. They should be within 10% of last time. Use -s (--skip-size-test) to skip
  # this test.
  if [[ $skip_size_test != 'skip' ]]
  then ./check_query_sizes.py
       if [[ $? != 0 ]]
       then exit -1
       fi
  fi


  for file in queries/*
  do
    if [[ 1 == `gstat -c %h $file` ]]
    then mv $file query_archive/
    fi
  done
  exit 0
 )
