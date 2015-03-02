#!/usr/bin/env bash

function select_example() {
  example_outvar=$1
  examples_dir=$2
  find_pattern=$3
  example_arg=$4
  default_file=$5

  if [[ -n ${example_arg} ]]; then
    if [[ -f ${example_arg} ]]; then
      example_file=${example_arg}
    else
      example_file="${examples_dir}/${example_arg}/${default_file}"
    fi
  fi

  all_examples=($(find ${examples_dir} -name "${find_pattern}"))
  while [[ -z ${example_file} || ! -f ${example_file} ]] ; do
    if [[ -n ${example_file} ]]; then
      echo "No example found at ${example_file}."
    fi
    echo "Please specify an example by its number."
    echo "Examples available:"
    LINE=0
    for e in ${all_examples[@]}; do
      LINE=$((LINE+1))
      REL_FILE=${e#${examples_dir}/}
      DESC=`grep 'description' $e | tail -1 | sed 's/"description"[^"]*"\([^"]*\)".*/\1/' `
      echo "${LINE} - ${REL_FILE} - ${DESC:-No Description}"
    done
    read -p "[1] > " NUM_SELECTED
    echo " "
    NUM_SELECTED=${NUM_SELECTED:-1}
    example_file=${all_examples[$((NUM_SELECTED-1))]}
  done
  eval $example_outvar="'$example_file'"
}
