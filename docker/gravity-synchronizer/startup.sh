#!/bin/bash

get_args() {
	_arg=$1
	shift
	echo "$@" | grep -Eo "\-\-${_arg}=[^ ]+" | cut -d= -f2 | tail -n 1
	unset _arg
}

[ "$#" -eq 0 ] || {
	rules=$(get_args rules "$@")
	[ "$rules" != "" ] && {
		ruleConfigPath="./rules/rules.json"

		[ "$GRAVITY_SYNCHRONIZER_RULES_RULECONFIG" != "" ] && {
			ruleConfigPath=$GRAVITY_SYNCHRONIZER_RULES_RULECONFIG
		} 

		echo $rules > $ruleConfigPath
	}

	db=$(get_args db "$@")
	[ "$db" != "" ] &&{
		dbConfigPath="./rules/database.json"

		[ "$GRAVITY_SYNCHRONIZER_RULES_DBCONFIG" != "" ] && {
			dbConfigPath=$GRAVITY_SYNCHRONIZER_RULES_DBCONFIG
		} 

		echo $db > $dbConfigPath
	}

	exporter=$(get_args exporter "$@")
	[ "$exporter" != "" ] &&{
		exporterConfigPath="./rules/exporter.json"

		[ "$GRAVITY_SYNCHRONIZER_RULES_EXPORTERCONFIG" != "" ] && {
			exporterConfigPath=$GRAVITY_SYNCHRONIZER_RULES_EXPORTERCONFIG
		} 

		echo $exporter > $exporterConfigPath
	}
}

/gravity-synchronizer
